package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/bestruirui/octopus/internal/utils/safe"
	"github.com/tmaxmax/go-sse"
)

// ErrEmptyUpstreamStream marks 200 SSE streams that ended without forwarding
// any payload (all events skipped by transform or no events at all).
// Relay should fail over to another channel.
var ErrEmptyUpstreamStream = errors.New("upstream stream ended without forwarding any payload")

// StreamEndReason records why a stream processing loop ended. The stream-close
// moment is the most informative observation point: "ended normally but empty"
// and "stream truncated mid-flight" demand different downstream semantics
// (retry vs. attribution), so relay-layer rulings must not treat them alike.
// Run() assigns the reason on every exit path; query via EndReason() after
// Run returns.
type StreamEndReason string

const (
	// StreamEndReasonDone: normal EOF with payload forwarded (success).
	StreamEndReasonDone StreamEndReason = "done"
	// StreamEndReasonEmpty: normal EOF but nothing was forwarded
	// (ErrEmptyUpstreamStream path).
	StreamEndReasonEmpty StreamEndReason = "empty"
	// StreamEndReasonClientGone: request context canceled (client disconnect)
	// without a terminal event in the buffered stream.
	StreamEndReasonClientGone StreamEndReason = "client_gone"
	// StreamEndReasonFirstTokenTimeout: first token timeout fired.
	StreamEndReasonFirstTokenTimeout StreamEndReason = "first_token_timeout"
	// StreamEndReasonReadError: source read failed (non-EOF).
	StreamEndReasonReadError StreamEndReason = "read_error"
	// StreamEndReasonTransformError: transform callback returned an error.
	StreamEndReasonTransformError StreamEndReason = "transform_error"
	// StreamEndReasonWriteError: client write failed.
	StreamEndReasonWriteError StreamEndReason = "write_error"
	// StreamEndReasonHeartbeatError: heartbeat write failed.
	StreamEndReasonHeartbeatError StreamEndReason = "heartbeat_error"
)

// StreamSource abstracts different event sources (SSE, WebSocket, raw bytes).
type StreamSource interface {
	// ReadEvent blocks until the next event is available or returns an error.
	// Returns io.EOF when the stream ends normally.
	ReadEvent(ctx context.Context) ([]byte, error)

	// Close releases resources. Must be idempotent.
	Close() error
}

// StreamTransform converts raw event data to the client's expected format.
// Returns nil/empty slice to skip writing (e.g., keep-alive events).
// For passthrough, set to nil in StreamConfig.
type StreamTransform func(ctx context.Context, data []byte) ([]byte, error)

// StreamWriter abstracts the HTTP/WebSocket response writer.
type StreamWriter interface {
	Write(data []byte) (int, error)
	Flush()
	Written() bool
	Header() http.Header
	WriteHeader(code int)
}

// StreamConfig configures a StreamProcessor instance.
type StreamConfig struct {
	// Core dependencies
	Source    StreamSource
	Transform StreamTransform // nil for passthrough
	Writer    StreamWriter
	Context   context.Context

	// Timeout & heartbeat
	FirstTokenTimeout time.Duration // 0 to disable
	HeartbeatInterval time.Duration // 0 to disable

	// Callbacks
	OnFirstToken func()                                            // Called when first payload written
	OnHeldChunk  func()                                            // Called when transform held a chunk (empty-output hold); used to re-arm first token timer
	OnFinish     func(ctx context.Context, rawStream []byte) error // Called on stream end

	// Passthrough-specific
	BufferRawStream bool                // Enable raw stream buffering for metrics
	TerminalEvents  map[string]struct{} // Protocol terminal events for early completion
}

// StreamProcessor unifies all stream handling logic.
type StreamProcessor struct {
	config StreamConfig

	// State
	rawBuffer      bytes.Buffer
	payloadWritten bool
	firstToken     bool
	endReason      StreamEndReason
}

// NewStreamProcessor creates a processor from config.
func NewStreamProcessor(config StreamConfig) *StreamProcessor {
	return &StreamProcessor{
		config:     config,
		firstToken: true,
	}
}

// Run executes the unified stream processing loop.
func (p *StreamProcessor) Run() error {
	// Set SSE response headers
	headers := p.config.Writer.Header()
	headers.Set("Content-Type", "text/event-stream")
	headers.Set("Cache-Control", "no-cache")
	headers.Set("Connection", "keep-alive")
	headers.Set("X-Accel-Buffering", "no")

	// Setup heartbeat ticker
	var heartbeatTicker *time.Ticker
	var heartbeatC <-chan time.Time
	if p.config.HeartbeatInterval > 0 {
		heartbeatTicker = time.NewTicker(p.config.HeartbeatInterval)
		heartbeatC = heartbeatTicker.C
		defer heartbeatTicker.Stop()
	}

	// Setup first token timeout
	var firstTokenTimer *time.Timer
	var firstTokenC <-chan time.Time
	if p.firstToken && p.config.FirstTokenTimeout > 0 {
		firstTokenTimer = time.NewTimer(p.config.FirstTokenTimeout)
		firstTokenC = firstTokenTimer.C
		defer func() {
			if firstTokenTimer != nil {
				firstTokenTimer.Stop()
			}
		}()
	}

	// Async read from source — use a derived context so we can unblock on any exit.
	readCtx, readCancel := context.WithCancel(p.config.Context)
	defer readCancel()
	defer p.config.Source.Close()

	type readResult struct {
		data []byte
		err  error
	}
	results := make(chan readResult, 1)
	safe.Go("stream-processor-read", func() {
		defer close(results)
		for {
			data, err := p.config.Source.ReadEvent(readCtx)
			select {
			case results <- readResult{data: data, err: err}:
			case <-readCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	})

	// Main event loop
	for {
		select {
		case <-p.config.Context.Done():
			return p.handleDisconnect()

		case <-firstTokenC:
			return p.handleFirstTokenTimeout()

		case <-heartbeatC:
			if err := p.writeHeartbeat(); err != nil {
				p.endReason = StreamEndReasonHeartbeatError
				return err
			}

		case r, ok := <-results:
			if !ok {
				// Channel closed, stream ended
				return p.finalize()
			}

			if r.err != nil {
				if r.err == io.EOF {
					return p.finalize()
				}
				p.endReason = StreamEndReasonReadError
				return fmt.Errorf("stream read error: %w", r.err)
			}

			if len(r.data) == 0 {
				continue
			}

			// Buffer raw data if enabled
			if p.config.BufferRawStream {
				p.rawBuffer.Write(r.data)
			}

			// Transform and write
			held, err := p.processEvent(r.data)
			if err != nil {
				return err
			}

			// 空输出保持：chunk 被 transform 跳过时重置首字定时器（processor-timer 模式；
			// budget 模式下 firstTokenTimer 为 nil，由 OnHeldChunk 回调重排预算定时器）。
			// 仅在 OnHeldChunk 已接线（空输出重试启用）时生效——OFF 等价要求 legacy
			// 跳过 chunk 保持既有定时器语义（不重排）。
			if held && p.config.OnHeldChunk != nil && firstTokenTimer != nil {
				if !firstTokenTimer.Stop() {
					select {
					case <-firstTokenTimer.C:
					default:
					}
				}
				firstTokenTimer.Reset(p.config.FirstTokenTimeout)
			}

			// First token handling
			if p.firstToken && p.payloadWritten {
				p.firstToken = false
				if p.config.OnFirstToken != nil {
					p.config.OnFirstToken()
				}
				if firstTokenTimer != nil {
					if !firstTokenTimer.Stop() {
						select {
						case <-firstTokenTimer.C:
						default:
						}
					}
					firstTokenTimer = nil
					firstTokenC = nil
				}
			}

		}
	}
}

// processEvent transforms and writes a single event.
// Returns held=true when the transform intentionally skipped the chunk
// (empty-output hold — nothing written, first-token timer may be re-armed).
func (p *StreamProcessor) processEvent(data []byte) (held bool, err error) {
	var output []byte

	if p.config.Transform != nil {
		output, err = p.config.Transform(p.config.Context, data)
		if err != nil {
			p.endReason = StreamEndReasonTransformError
			return false, fmt.Errorf("transform error: %w", err)
		}
		if len(output) == 0 {
			// 空输出保持：transform 有意跳过该 chunk（未写入客户端），
			// 通知回调重排首字计时，避免推理模型长 reasoning 期间被首字超时误杀。
			if p.config.OnHeldChunk != nil {
				p.config.OnHeldChunk()
			}
			return true, nil // Skip empty output
		}
	} else {
		output = data // Passthrough
	}

	if _, err := p.config.Writer.Write(output); err != nil {
		p.endReason = StreamEndReasonWriteError
		return false, fmt.Errorf("write error: %w", err)
	}

	p.payloadWritten = true
	p.config.Writer.Flush()
	return false, nil
}

// writeHeartbeat sends SSE heartbeat (comment line).
func (p *StreamProcessor) writeHeartbeat() error {
	if _, err := p.config.Writer.Write([]byte(":\n\n")); err != nil {
		return err
	}
	p.config.Writer.Flush()
	return nil
}

// handleDisconnect handles context cancellation or timeout.
func (p *StreamProcessor) handleDisconnect() error {
	// Default reason for a disconnect; overwritten below when the buffered
	// stream had already reached a terminal event (that finalize() run then
	// records done/empty, which is the truer description of the upstream side).
	p.endReason = StreamEndReasonClientGone

	// Check for terminal events in buffered stream
	if p.config.BufferRawStream && len(p.config.TerminalEvents) > 0 {
		if p.streamReachedTerminal() {
			log.Debugf("client disconnected but stream reached terminal event, treating as success")
			return p.finalize()
		}
	}

	err := p.config.Context.Err()
	log.Debugf("client disconnected, stopping stream: written=%t first_token_seen=%t err=%v",
		p.payloadWritten, !p.firstToken, err)

	if p.config.BufferRawStream && p.rawBuffer.Len() > 0 {
		// Still call OnFinish to collect partial metrics
		if p.config.OnFinish != nil {
			_ = p.config.OnFinish(context.Background(), p.rawBuffer.Bytes())
		}
	}

	return err
}

// handleFirstTokenTimeout returns first token timeout error.
func (p *StreamProcessor) handleFirstTokenTimeout() error {
	p.endReason = StreamEndReasonFirstTokenTimeout
	log.Warnf("first token timeout (%v), switching channel", p.config.FirstTokenTimeout)
	return fmt.Errorf("first token timeout after %v", p.config.FirstTokenTimeout)
}

// finalize completes the stream and calls OnFinish callback.
func (p *StreamProcessor) finalize() error {
	if !p.payloadWritten {
		p.endReason = StreamEndReasonEmpty
		return ErrEmptyUpstreamStream
	}

	p.endReason = StreamEndReasonDone

	log.Debugf("stream end (payload_written=%t)", p.payloadWritten)

	if p.config.OnFinish != nil {
		rawStream := p.rawBuffer.Bytes()
		if err := p.config.OnFinish(p.config.Context, rawStream); err != nil {
			return err
		}
	}

	return nil
}

// PayloadWritten returns whether any payload has been written to the client.
func (p *StreamProcessor) PayloadWritten() bool {
	return p.payloadWritten
}

// EndReason returns why the processing loop ended. Empty string until Run
// reaches an exit path; query after Run returns. OnFinish callbacks see the
// reason already assigned (finalize assigns before invoking OnFinish).
func (p *StreamProcessor) EndReason() StreamEndReason {
	return p.endReason
}

// streamReachedTerminal checks if buffered stream contains a terminal event.
func (p *StreamProcessor) streamReachedTerminal() bool {
	if p.rawBuffer.Len() == 0 {
		return false
	}

	readCfg := &sse.ReadConfig{MaxEventSize: 32 * 1024 * 1024}
	for ev, err := range sse.Read(bytes.NewReader(p.rawBuffer.Bytes()), readCfg) {
		if err != nil {
			break
		}

		// Extract event type
		typ := strings.TrimSpace(ev.Type)
		if typ == "" {
			data := ev.Data
			if len(data) > 0 && data[0] == '{' {
				typ = extractJSONType(data)
			}
		}

		if _, ok := p.config.TerminalEvents[typ]; ok {
			return true
		}
	}

	return false
}

// extractJSONType extracts "type" field from JSON without full unmarshaling.
func extractJSONType(data string) string {
	// Simple extraction: find "type":"value"
	if idx := strings.Index(data, `"type"`); idx >= 0 {
		rest := data[idx+6:]
		if idx := strings.IndexByte(rest, ':'); idx >= 0 {
			rest = rest[idx+1:]
			rest = strings.TrimSpace(rest)
			if len(rest) > 0 && rest[0] == '"' {
				rest = rest[1:]
				if idx := strings.IndexByte(rest, '"'); idx >= 0 {
					return rest[:idx]
				}
			}
		}
	}
	return ""
}
