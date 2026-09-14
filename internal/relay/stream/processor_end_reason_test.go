package stream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Characterization for StreamEndReason (G3): every Run() exit path records the
// reason; query via EndReason() after Run returns. The reason is the relay's
// discriminative observation for "ended normally but empty" vs "truncated
// mid-flight".

func TestStreamEndReasonDoneOnNormalEOF(t *testing.T) {
	source := newMockStreamSource([][]byte{[]byte(`chunk1`), []byte(`chunk2`)})
	writer := newMockStreamWriter()

	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: context.Background(),
	})
	if err := processor.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := processor.EndReason(); got != StreamEndReasonDone {
		t.Fatalf("expected done, got %q", got)
	}
}

func TestStreamEndReasonEmptyOnSkippedStream(t *testing.T) {
	source := newMockStreamSource([][]byte{[]byte(`e1`), []byte(`e2`)})
	writer := newMockStreamWriter()

	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: context.Background(),
		Transform: func(ctx context.Context, data []byte) ([]byte, error) {
			return nil, nil // skip everything
		},
	})
	err := processor.Run()
	if !errors.Is(err, ErrEmptyUpstreamStream) {
		t.Fatalf("expected ErrEmptyUpstreamStream, got %v", err)
	}
	if got := processor.EndReason(); got != StreamEndReasonEmpty {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestStreamEndReasonClientGoneOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	source := &cancelTestSource{first: []byte(`chunk1`)}
	writer := newMockStreamWriter()

	firstTokenSeen := make(chan struct{})
	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: ctx,
		OnFirstToken: func() {
			close(firstTokenSeen)
		},
	})
	errChan := make(chan error, 1)
	go func() {
		errChan <- processor.Run()
	}()
	<-firstTokenSeen
	cancel()

	err := <-errChan
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if got := processor.EndReason(); got != StreamEndReasonClientGone {
		t.Fatalf("expected client_gone, got %q", got)
	}
}

func TestStreamEndReasonFirstTokenTimeout(t *testing.T) {
	source := &slowTestSource{delay: 200 * time.Millisecond}
	writer := newMockStreamWriter()

	processor := NewStreamProcessor(StreamConfig{
		Source:            source,
		Writer:            writer,
		Context:           context.Background(),
		FirstTokenTimeout: 20 * time.Millisecond,
	})
	err := processor.Run()
	if err == nil || !strings.Contains(err.Error(), "first token timeout") {
		t.Fatalf("expected first token timeout error, got %v", err)
	}
	if got := processor.EndReason(); got != StreamEndReasonFirstTokenTimeout {
		t.Fatalf("expected first_token_timeout, got %q", got)
	}
}

func TestStreamEndReasonTransformError(t *testing.T) {
	source := newMockStreamSource([][]byte{[]byte(`bad`)})
	writer := newMockStreamWriter()

	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: context.Background(),
		Transform: func(ctx context.Context, data []byte) ([]byte, error) {
			return nil, errors.New("boom")
		},
	})
	err := processor.Run()
	if err == nil || !strings.Contains(err.Error(), "transform error") {
		t.Fatalf("expected transform error, got %v", err)
	}
	if got := processor.EndReason(); got != StreamEndReasonTransformError {
		t.Fatalf("expected transform_error, got %q", got)
	}
}

func TestStreamEndReasonWriteError(t *testing.T) {
	source := newMockStreamSource([][]byte{[]byte(`chunk1`)})

	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  &failingStreamWriter{},
		Context: context.Background(),
	})
	err := processor.Run()
	if err == nil || !strings.Contains(err.Error(), "write error") {
		t.Fatalf("expected write error, got %v", err)
	}
	if got := processor.EndReason(); got != StreamEndReasonWriteError {
		t.Fatalf("expected write_error, got %q", got)
	}
}

func TestStreamEndReasonEmptyBeforeOnFinishSeesReason(t *testing.T) {
	// finalize assigns the reason BEFORE invoking OnFinish, so the passthrough
	// empty_stream warning line can carry it.
	source := newMockStreamSource([][]byte{[]byte(`e1`)})
	writer := newMockStreamWriter()

	seenInOnFinish := make(chan StreamEndReason, 1)
	var processor *StreamProcessor
	processor = NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: context.Background(),
		OnFinish: func(ctx context.Context, rawStream []byte) error {
			seenInOnFinish <- processor.EndReason()
			return nil
		},
	})
	if err := processor.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := <-seenInOnFinish; got != StreamEndReasonDone {
		t.Fatalf("expected OnFinish to see done, got %q", got)
	}
}

// slowTestSource blocks for `delay` before returning EOF.
type slowTestSource struct {
	delay time.Duration
}

func (s *slowTestSource) ReadEvent(ctx context.Context) ([]byte, error) {
	select {
	case <-time.After(s.delay):
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *slowTestSource) Close() error { return nil }

// failingStreamWriter fails every Write.
type failingStreamWriter struct{}

func (f *failingStreamWriter) Write(data []byte) (int, error) {
	return 0, errors.New("client write failed")
}
func (f *failingStreamWriter) Flush()        {}
func (f *failingStreamWriter) Written() bool { return false }
func (f *failingStreamWriter) Header() http.Header {
	return make(http.Header)
}
func (f *failingStreamWriter) WriteHeader(code int) {}
