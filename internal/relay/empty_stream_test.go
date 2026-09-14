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

// 生产事故签名（2026-09-14 日志取证，log.txt）：上游桥（LiteLLM）把 429 洗成 200
// SSE 空壳流——response.created → response.completed，零输出事件、零 usage——
// 载荷先行透传、处理器返回 nil、success=true 记账。空输出重试机制（transform 路径
// 专属）在这条 passthrough 路径上不生效。观测底线：该形态必须产出
// relay.empty_stream 告警（kind=terminal_no_output），不再静默。
// 行为保持不变：nil 返回值与 verbatim 透传都是表征的一部分。
func TestPassthroughShellStreamTerminalNoOutputIsLoggedButRecordedSuccess(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIResponse, transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	observedCore, observed := observer.New(zapcore.WarnLevel)
	prevLogger := log.Logger
	log.Logger = zap.New(observedCore).Sugar()
	defer func() { log.Logger = prevLogger }()

	pt := ra.outAdapter.(transformerModel.PassthroughCapable)
	cfg := pt.PassthroughConfig()
	// 与事故响应同构（id/model 以测试值替换）：created + completed，无 output_item、
	// 无 delta、usage 缺失。
	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_shell","object":"response","model":"gpt-5.6-sol","created_at":0,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_shell","object":"response","model":"gpt-5.6-sol","created_at":0,"output":[],"status":"completed"}}`,
		"",
	}, "\n")

	if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), cfg); err != nil {
		t.Fatalf("shell passthrough stream must keep returning nil (no behavior change), got %v", err)
	}
	if recorder.Body.Len() == 0 {
		t.Fatalf("expected shell payload forwarded verbatim, got empty body")
	}
	if !ra.streamPayloadWritten.Load() {
		t.Fatalf("expected payloadWritten=true after forwarding shell payload")
	}

	entries := observed.FilterMessage("relay.empty_stream").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly one relay.empty_stream warn for shell stream, got %d", len(entries))
	}
	fields := map[string]string{}
	for _, f := range entries[0].Context {
		fields[f.Key] = f.String
	}
	if fields["empty_stream_kind"] != "terminal_no_output" {
		t.Fatalf("expected empty_stream_kind=terminal_no_output, got %q (fields: %v)", fields["empty_stream_kind"], fields)
	}
	// G3: stream_end_reason 携带流结束方式——正常 EOF（finalize 先赋值再调
	// OnFinish，闭包内可见）。
	if fields["stream_end_reason"] != "done" {
		t.Fatalf("expected stream_end_reason=done, got %q (fields: %v)", fields["stream_end_reason"], fields)
	}
	if fields["model"] != "gpt-4o" {
		t.Fatalf("expected model field gpt-4o (requestModel from test helper), got %q", fields["model"])
	}
}

// 对照组：携带输出事件的合法完成流（如 reasoning summary delta + completed）不得
// 触发 terminal_no_output 告警——信封级证据存在即豁免，保证推理模型不被误伤。
func TestPassthroughStreamWithOutputEvidenceNotLogged(t *testing.T) {
	ra, _ := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIResponse, transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	observedCore, observed := observer.New(zapcore.WarnLevel)
	prevLogger := log.Logger
	log.Logger = zap.New(observedCore).Sugar()
	defer func() { log.Logger = prevLogger }()

	pt := ra.outAdapter.(transformerModel.PassthroughCapable)
	cfg := pt.PassthroughConfig()
	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_r","object":"response","model":"gpt-5.6-sol","created_at":0,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.reasoning_summary_text.delta","delta":"thinking"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_r","object":"response","model":"gpt-5.6-sol","created_at":0,"output":[],"status":"completed","usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}`,
		"",
	}, "\n")

	if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), cfg); err != nil {
		t.Fatalf("evidence-carrying stream must succeed, got %v", err)
	}
	entries := observed.FilterMessage("relay.empty_stream").All()
	if len(entries) != 0 {
		t.Fatalf("expected no relay.empty_stream warn for evidence-carrying stream, got %d", len(entries))
	}
}

// observeEmptyStreamUsage 的三种 usage 形态（影子判别谓词的输入侧）：
// 在场为零（缺陷触发形态）/ 在场为正（豁免）/ 缺失（NULL≠0，未知）。
func TestObserveEmptyStreamUsageForms(t *testing.T) {
	created := `data: {"type":"response.created","response":{"id":"r","status":"in_progress"}}` + "\n\n"
	cases := []struct {
		name      string
		rawStream string
		want      emptyUsageVerdict
	}{
		{
			name: "usage present and zero",
			rawStream: created + `data: {"type":"response.completed","response":{"id":"r","status":"completed","usage":{"input_tokens":194495,"output_tokens":0,"total_tokens":194495}}}` + "\n\n",
			want:  emptyUsageZero,
		},
		{
			name: "usage present and positive",
			rawStream: created + `data: {"type":"response.completed","response":{"id":"r","status":"completed","usage":{"input_tokens":10,"output_tokens":9964,"total_tokens":9974,"output_tokens_details":{"reasoning_tokens":9964}}}}` + "\n\n",
			want:  emptyUsagePositive,
		},
		{
			name:      "usage missing entirely",
			rawStream: created + `data: {"type":"response.completed","response":{"id":"r","status":"completed"}}` + "\n\n",
			want:      emptyUsageAbsent,
		},
		{
			name:      "empty stream",
			rawStream: "",
			want:      emptyUsageAbsent,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := observeEmptyStreamUsage([]byte(tc.rawStream)); got != tc.want {
				t.Fatalf("observeEmptyStreamUsage() = %v, want %v", got, tc.want)
			}
		})
	}
}

// 影子判别端到端：事故形态（空壳流、usage 缺失）必须记 relay.empty_stream_shadow
// usage_form=usage_absent——这条日志就是影子期实测「桥透不透 usage」的数据来源。
// 行为零变更：nil 返回值、verbatim 透传、记账照旧。
func TestPassthroughShellStreamShadowLogged(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIResponse, transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	observedCore, observed := observer.New(zapcore.InfoLevel)
	prevLogger := log.Logger
	log.Logger = zap.New(observedCore).Sugar()
	defer func() { log.Logger = prevLogger }()

	pt := ra.outAdapter.(transformerModel.PassthroughCapable)
	cfg := pt.PassthroughConfig()
	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_shell","object":"response","model":"gpt-5.6-sol","created_at":0,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_shell","object":"response","model":"gpt-5.6-sol","created_at":0,"output":[],"status":"completed"}}`,
		"",
	}, "\n")

	if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), cfg); err != nil {
		t.Fatalf("shell passthrough stream must keep returning nil (no behavior change), got %v", err)
	}
	if recorder.Body.Len() == 0 {
		t.Fatalf("expected shell payload forwarded verbatim, got empty body")
	}

	entries := observed.FilterMessage("relay.empty_stream_shadow").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly one relay.empty_stream_shadow info, got %d", len(entries))
	}
	fields := map[string]string{}
	for _, f := range entries[0].Context {
		fields[f.Key] = f.String
	}
	if fields["usage_form"] != "usage_absent" {
		t.Fatalf("expected usage_form=usage_absent, got %q (fields: %v)", fields["usage_form"], fields)
	}
}

// 影子判别对照组：usage 在场且 output_tokens==0（缺陷的在场形态）记 usage_zero；
// usage 在场且 output_tokens>0（与零可见矛盾的豁免形态）不记。
func TestPassthroughShellStreamShadowForms(t *testing.T) {
	created := `data: {"type":"response.created","response":{"id":"resp_s","object":"response","model":"m","created_at":0,"output":[],"status":"in_progress"}}` + "\n\n"

	t.Run("usage zero is shadow-logged", func(t *testing.T) {
		ra, _ := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIResponse, transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)
		observedCore, observed := observer.New(zapcore.InfoLevel)
		prevLogger := log.Logger
		log.Logger = zap.New(observedCore).Sugar()
		defer func() { log.Logger = prevLogger }()

		pt := ra.outAdapter.(transformerModel.PassthroughCapable)
		cfg := pt.PassthroughConfig()
		body := created + `data: {"type":"response.completed","response":{"id":"resp_s","object":"response","model":"m","created_at":0,"output":[],"status":"completed","usage":{"input_tokens":194495,"output_tokens":0,"total_tokens":194495}}}` + "\n\n"
		if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), cfg); err != nil {
			t.Fatalf("shell stream must keep returning nil, got %v", err)
		}
		entries := observed.FilterMessage("relay.empty_stream_shadow").All()
		if len(entries) != 1 {
			t.Fatalf("expected exactly one shadow log, got %d", len(entries))
		}
		fields := map[string]string{}
		for _, f := range entries[0].Context {
			fields[f.Key] = f.String
		}
		if fields["usage_form"] != "usage_zero" {
			t.Fatalf("expected usage_form=usage_zero, got %q", fields["usage_form"])
		}
	})

	t.Run("usage positive is exempt", func(t *testing.T) {
		ra, _ := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIResponse, transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)
		observedCore, observed := observer.New(zapcore.InfoLevel)
		prevLogger := log.Logger
		log.Logger = zap.New(observedCore).Sugar()
		defer func() { log.Logger = prevLogger }()

		pt := ra.outAdapter.(transformerModel.PassthroughCapable)
		cfg := pt.PassthroughConfig()
		body := created + `data: {"type":"response.completed","response":{"id":"resp_s","object":"response","model":"m","created_at":0,"output":[],"status":"completed","usage":{"input_tokens":10,"output_tokens":9964,"total_tokens":9974}}}` + "\n\n"
		if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), cfg); err != nil {
			t.Fatalf("exempt stream must keep returning nil, got %v", err)
		}
		entries := observed.FilterMessage("relay.empty_stream_shadow").All()
		if len(entries) != 0 {
			t.Fatalf("expected no shadow log for usage-positive exempt stream, got %d", len(entries))
		}
	})
}
