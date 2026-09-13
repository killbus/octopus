package relay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/relay/stream"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// Issue #65 Bug 2: a 200 SSE stream that ends without forwarding any payload
// used to return nil from the stream handlers, so attempt() recorded a
// zero-token success, reset the circuit breaker, and pinned stickiness to the
// misbehaving channel. Empty streams must fail the attempt so the relay can
// fail over (nothing was written to the client yet).

func newEmptyStreamTestAttempt(t *testing.T, inType inbound.InboundType, rawFormat transformerModel.APIFormat, outType outbound.OutboundType) (*relayAttempt, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/test", nil)
	internalReq := &transformerModel.InternalLLMRequest{
		Model:        "gpt-4o",
		Stream:       boolPtr(true),
		RawAPIFormat: rawFormat,
	}
	req := &relayRequest{
		c:               c,
		inAdapter:       inbound.Get(inType),
		internalRequest: internalReq,
		metrics:         NewRelayMetrics(1, internalReq.Model, nil, internalReq),
		apiKeyID:        1,
		requestModel:    internalReq.Model,
	}
	return &relayAttempt{
		relayRequest: req,
		outAdapter:   outbound.Get(outType),
	}, recorder
}

func sseTestResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(bytes.NewReader([]byte(body))),
	}
}

func TestHandleStreamResponseEmptyStreamFails(t *testing.T) {
	ra, _ := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIChat, transformerModel.APIFormatOpenAIChatCompletion, outbound.OutboundTypeOpenAIResponse)

	err := ra.handleStreamResponseV2(context.Background(), sseTestResponse(""))
	if !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("expected stream.ErrEmptyUpstreamStream for empty stream, got %v", err)
	}
}

func TestHandleStreamResponseUnconvertibleEventsOnlyFails(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIChat, transformerModel.APIFormatOpenAIChatCompletion, outbound.OutboundTypeOpenAIResponse)

	// Unknown Responses event types produce zero stream events, so nothing is
	// ever forwarded even though the stream carried data lines.
	body := strings.Join([]string{
		`data: {"type":"response.queue_position","position":1}`,
		"",
		`data: {"type":"response.another_unknown_event"}`,
		"",
	}, "\n")
	err := ra.handleStreamResponseV2(context.Background(), sseTestResponse(body))
	if !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("expected stream.ErrEmptyUpstreamStream for unconvertible-only stream, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected nothing forwarded to client, got %q", recorder.Body.String())
	}
}

func TestHandleStreamResponseWithPayloadSucceeds(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIChat, transformerModel.APIFormatOpenAIChatCompletion, outbound.OutboundTypeOpenAIResponse)

	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"hello"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
		"",
	}, "\n")
	if err := ra.handleStreamResponseV2(context.Background(), sseTestResponse(body)); err != nil {
		t.Fatalf("expected stream with payload to succeed, got %v", err)
	}
	if recorder.Body.Len() == 0 {
		t.Fatalf("expected forwarded payload, got empty body")
	}
}

func TestPassthroughOpenAIResponsesEmptyStreamFails(t *testing.T) {
	ra, _ := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIResponse, transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	pt := ra.outAdapter.(transformerModel.PassthroughCapable)
	cfg := pt.PassthroughConfig()
	err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(""), cfg)
	if !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("expected stream.ErrEmptyUpstreamStream for empty passthrough stream, got %v", err)
	}
}

func TestPassthroughAnthropicEmptyStreamFails(t *testing.T) {
	ra, _ := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)

	pt := ra.outAdapter.(transformerModel.PassthroughCapable)
	cfg := pt.PassthroughConfig()
	err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(""), cfg)
	if !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("expected stream.ErrEmptyUpstreamStream for empty passthrough stream, got %v", err)
	}
}

// Issue #65 / empty-output-retry port PR-1: a passthrough stream whose payload is an
// error event (e.g. `response.failed`) is forwarded verbatim, sets payloadWritten=true,
// and is recorded as success — the observational inversion this PR makes visible via
// the relay.empty_stream Warnw without changing any behavior.
func TestPassthroughErrorEventStreamIsLoggedButRecordedSuccess(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIResponse, transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	// Capture Warnw output so the characterization can assert the log contract.
	observedCore, observed := observer.New(zapcore.WarnLevel)
	prevLogger := log.Logger
	log.Logger = zap.New(observedCore).Sugar()
	defer func() { log.Logger = prevLogger }()

	pt := ra.outAdapter.(transformerModel.PassthroughCapable)
	cfg := pt.PassthroughConfig()
	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.failed","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"failed","error":{"code":429,"message":"rate limited"}}}`,
		"",
	}, "\n")

	// Zero behavior change: handler returns nil — the value the attempt loop feeds
	// to SaveWithChannelStats(success=true) — and the error payload is forwarded to
	// the client as-is.
	if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), cfg); err != nil {
		t.Fatalf("error-event passthrough stream must keep returning nil (no behavior change), got %v", err)
	}
	if recorder.Body.Len() == 0 {
		t.Fatalf("expected error-event payload forwarded verbatim, got empty body")
	}
	// First half of the observational inversion: bytes reached the client, so the
	// processor marked the payload as written even though it is an upstream error.
	if !ra.streamPayloadWritten.Load() {
		t.Fatalf("expected payloadWritten=true after forwarding error-event payload")
	}

	entries := observed.FilterMessage("relay.empty_stream").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly one relay.empty_stream warn, got %d", len(entries))
	}
	fields := map[string]string{}
	for _, f := range entries[0].Context {
		fields[f.Key] = f.String
	}
	if fields["empty_stream_kind"] != "error_event" {
		t.Fatalf("expected empty_stream_kind=error_event, got %q (fields: %v)", fields["empty_stream_kind"], fields)
	}
	if fields["model"] != "gpt-4o" {
		t.Fatalf("expected model field gpt-4o, got %q", fields["model"])
	}
}
