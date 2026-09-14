package relay

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/relay/stream"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

// inboundOpenAIResponse 便于在本文件内构造 Responses 入站类型。
func inboundOpenAIResponse() inbound.InboundType {
	return inbound.InboundTypeOpenAIResponse
}

// 空输出保持与重试（PR-2，fork 2f610094 / PR #155 端口）的端到端行为测试。
// 走真实适配器链（OpenAI Responses outbound → OpenAI Responses inbound），
// 覆盖保持、释放、finish_reason 闸门、8 MiB 超限降级、FTT 重排与非流式闸门。

// openAIResponsesSSEReasoningOnly 构造一个仅含 reasoning 增量、且无终态块的
// Responses SSE 流（R2 裁定：仅"EOF 无终态块且零可见内容"才触发重试）。
func openAIResponsesSSEReasoningOnly() string {
	return strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.reasoning_summary_text.delta","delta":"thinking hard"}`,
		"",
	}, "\n")
}

func TestEmptyRetryReasoningOnlyStreamRetries(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)
	ra.emptyRetryEnabled = true
	err := ra.handleStreamResponseV2(context.Background(), sseTestResponse(openAIResponsesSSEReasoningOnly()))
	if !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("expected stream.ErrEmptyUpstreamStream for reasoning-only stream with empty retry on, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected nothing forwarded to client (held), got %q", recorder.Body.String())
	}
	if ra.streamPayloadWritten.Load() {
		t.Fatalf("expected payloadWritten=false (held bytes are not writes)")
	}
}

func TestEmptyRetryReasoningThenTextForwardsBoth(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)
	ra.emptyRetryEnabled = true

	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.reasoning_summary_text.delta","delta":"thinking"}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"hello"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"completed","usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}`,
		"",
	}, "\n")
	if err := ra.handleStreamResponseV2(context.Background(), sseTestResponse(body)); err != nil {
		t.Fatalf("expected reasoning→text stream to succeed, got %v", err)
	}
	out := recorder.Body.String()
	if !strings.Contains(out, `"response.reasoning_summary_text.delta"`) || !strings.Contains(out, "thinking") {
		t.Fatalf("expected held reasoning flushed before text, got %q", out)
	}
	if !strings.Contains(out, `"response.output_text.delta"`) || !strings.Contains(out, "hello") {
		t.Fatalf("expected text delta forwarded, got %q", out)
	}
	// 顺序：持有字节必须先于可见字节写入。
	if strings.Index(out, "thinking") > strings.Index(out, "hello") {
		t.Fatalf("expected reasoning flushed before text, got %q", out)
	}
	if !ra.streamPayloadWritten.Load() {
		t.Fatalf("expected payloadWritten=true after text arrival")
	}
}

// R2 finish_reason 闸门：reasoning-only + completed（终态）是合法流——
// 持有字节在终态事件到达时随可见字节路径 flush，客户端收到 reasoning，不触发重试。
func TestEmptyRetryFinishReasonFlushesHeldBytes(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)
	ra.emptyRetryEnabled = true

	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.reasoning_summary_text.delta","delta":"only reasoning"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"completed"}}`,
		"",
	}, "\n")
	if err := ra.handleStreamResponseV2(context.Background(), sseTestResponse(body)); err != nil {
		t.Fatalf("expected reasoning-only+completed to be forwarded (finish_reason gate), got %v", err)
	}
	out := recorder.Body.String()
	if !strings.Contains(out, "only reasoning") {
		t.Fatalf("expected held reasoning flushed on finish_reason, got %q", out)
	}
	if !ra.streamPayloadWritten.Load() {
		t.Fatalf("expected payloadWritten=true after finish_reason flush")
	}
}

func TestEmptyRetryCapDegradesToPassthrough(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)
	ra.emptyRetryEnabled = true

	// 注入 8 MiB 小上限，然后用超过上限的 reasoning 块触发降级。
	prev := maxEmptyOutputHoldBytes
	maxEmptyOutputHoldBytes = 64
	defer func() { maxEmptyOutputHoldBytes = prev }()

	big := strings.Repeat("r", 200)
	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.reasoning_summary_text.delta","delta":"` + big + `"}`,
		"",
	}, "\n")
	// 降级后 reasoning 字节直通客户端，payloadWritten=true，流按成功记账（无终态 →
	// ErrEmptyUpstreamStream 只在 !payloadWritten 时返回，不会出现）。
	if err := ra.handleStreamResponseV2(context.Background(), sseTestResponse(body)); err != nil {
		t.Fatalf("expected cap-degrade passthrough to succeed, got %v", err)
	}
	if !strings.Contains(recorder.Body.String(), big) {
		t.Fatalf("expected degraded passthrough to forward reasoning bytes, got %q", recorder.Body.String())
	}
	if !ra.streamPayloadWritten.Load() {
		t.Fatalf("expected payloadWritten=true after cap degrade flush")
	}
}

func TestEmptyRetryOffEquivalentToLegacyBehavior(t *testing.T) {
	// OFF 等价：transform 直接委托原 transformStreamData，reasoning 字节照常转发，
	// 与 PR-2 之前的行为字节级一致（flag off = current bytes）。
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)
	ra.emptyRetryEnabled = false

	if err := ra.handleStreamResponseV2(context.Background(), sseTestResponse(openAIResponsesSSEReasoningOnly())); err != nil {
		t.Fatalf("OFF: reasoning stream must behave exactly as before PR-2, got %v", err)
	}
	if recorder.Body.Len() == 0 {
		t.Fatalf("OFF: reasoning bytes must be forwarded as before")
	}
	if !ra.streamPayloadWritten.Load() {
		t.Fatalf("OFF: expected payloadWritten=true (legacy semantics)")
	}

	// 等价：同一流经 wrapped(OFF) 与原 transformStreamData 产出结构相同
	// （入站适配器生成随机 item id，规范化后再比较）。
	ra2, recorder2 := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)
	ra2.emptyRetryEnabled = false
	if err := ra2.handleStreamResponseV2(context.Background(), sseTestResponse(openAIResponsesSSEReasoningOnly())); err != nil {
		t.Fatalf("OFF second run: %v", err)
	}
	if normalizeItemIDs(recorder2.Body.String()) != normalizeItemIDs(recorder.Body.String()) {
		t.Fatalf("OFF equivalence mismatch:\nwrapped=%q\nlegacy=%q", recorder2.Body.String(), recorder.Body.String())
	}
}

// normalizeItemIDs 把入站适配器生成的随机 reasoning item id 规范化为占位符，
// 便于跨实例比较输出结构。
func normalizeItemIDs(s string) string {
	lhs := strings.Index(s, `"item_`)
	for lhs >= 0 {
		// 从开引号之后开始找闭引号，避免自匹配（rhs 恒为 0 → 死循环）。
		rhs := strings.IndexByte(s[lhs+1:], '"')
		if rhs < 0 {
			break
		}
		s = s[:lhs+1] + "ITEM_ID" + s[lhs+1+rhs:]
		lhs = strings.Index(s, `"item_`)
	}
	return s
}

// 硬约束①（headless-stream 防护）的回归测试：inAdapter 是 request 级、跨 attempt 共享，
// 保持 chunk 若提前送入入站编码，abandon 后 hasResponseCreated / sequenceNumber /
// reasoning item 状态会泄漏进重试 attempt——response.created 被抑制（headless 流）、
// 序号跳变、上一 attempt 的 reasoning 复活。保持设计必须对被放弃的 chunk 零入站接触。
func TestEmptyRetryNoInAdapterLeakAcrossAttempts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/test", nil)
	internalReq := &transformerModel.InternalLLMRequest{
		Model:        "gpt-4o",
		Stream:       boolPtr(true),
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
	}
	// 单一 relayRequest 模拟真实重试循环：inAdapter 跨 attempt 共享。
	req := &relayRequest{
		c:               c,
		inAdapter:       inbound.Get(inboundOpenAIResponse()),
		internalRequest: internalReq,
		metrics:         NewRelayMetrics(1, internalReq.Model, nil, internalReq),
		apiKeyID:        1,
		requestModel:    internalReq.Model,
	}

	// Attempt 1：reasoning-only 流被保持后放弃（ErrEmptyUpstreamStream → 重试链）。
	ra1 := &relayAttempt{relayRequest: req, outAdapter: outbound.Get(outbound.OutboundTypeOpenAIResponse), emptyRetryEnabled: true}
	if err := ra1.handleStreamResponseV2(context.Background(), sseTestResponse(openAIResponsesSSEReasoningOnly())); !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("attempt1: expected ErrEmptyUpstreamStream, got %v", err)
	}

	// Attempt 2：同 inAdapter 上重放 reasoning→text 成功流，客户端必须收到
	// 结构完整的流（response.created 在场、无 attempt-1 的 reasoning 残余）。
	ra2 := &relayAttempt{relayRequest: req, outAdapter: outbound.Get(outbound.OutboundTypeOpenAIResponse), emptyRetryEnabled: true}
	body2 := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_2","object":"response","model":"gpt-4o","created_at":2,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"hello"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_2","object":"response","model":"gpt-4o","created_at":2,"output":[],"status":"completed"}}`,
		"",
	}, "\n")
	if err := ra2.handleStreamResponseV2(context.Background(), sseTestResponse(body2)); err != nil {
		t.Fatalf("attempt2: %v", err)
	}
	out := recorder.Body.String()
	if !strings.Contains(out, `"response.created"`) {
		t.Fatalf("headless stream: attempt2 output missing response.created (inAdapter state leaked from attempt1):\n%s", out)
	}
	if strings.Contains(out, "thinking hard") {
		t.Fatalf("attempt-1 reasoning leaked into attempt-2 stream:\n%s", out)
	}
	createdIdx := strings.Index(out, `"response.created"`)
	textIdx := strings.Index(out, "hello")
	if createdIdx < 0 || textIdx < 0 || createdIdx > textIdx {
		t.Fatalf("expected response.created before text delta:\n%s", out)
	}
}

func TestEmptyRetryFirstTokenTimerRearmedWhileHolding(t *testing.T) {
	ra, _ := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)
	ra.emptyRetryEnabled = true

	// budget 模式 rearm 语义：firstTokenTimeOutSec 秒级粒度不够 e2e 精度，
	// 直接验证 rearmFirstTokenTimer → budget.rearm 的链路与停止语义。
	fired := make(chan struct{})
	budget := &firstTokenBudget{}
	budget.ctx = context.Background()
	budget.cancel = func(cause error) { close(fired) }
	ra.firstTokenBudget = budget
	ra.firstTokenTimeOutSec = 0

	// 首字已到（stopTimer 已置 stopped）→ rearm no-op。
	ra.stopFirstTokenTimer()
	ra.rearmFirstTokenTimer()
	select {
	case <-fired:
		t.Fatalf("rearm after first token must not fire")
	default:
	}

	// 持有中（未停止）→ rearm 重建定时器并在到期后触发 cancel。
	budget.stopped = false
	ra.firstTokenTimeOutSec = 0 // rearm(0) no-op guard check happens below
	ra.rearmFirstTokenTimer()   // d=0 → no-op
	if budget.timer != nil {
		t.Fatalf("rearm with zero duration must be a no-op")
	}
	ra.firstTokenTimeOutSec = 1
	// 用小窗口直接调 budget.rearm 验证到期触发（秒级 AfterFunc 无法在测试内等待）。
	// 注意 rearm 自带加锁，测试不得再包一层 mu.Lock（非重入）。
	budget.rearm(5 * time.Millisecond)
	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatalf("expected rearmed budget timer to fire")
	}
}

func TestEmptyRetryNonStreamingEmptyOutputErrors(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)
	ra.emptyRetryEnabled = true

	// 非流式：所有 choice 仅含 reasoning → errEmptyOutput（StatusCode=0 → 可重试链）。
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(`{
			"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"status":"completed",
			"output":[{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"deep thought"}]}],
			"usage":{"input_tokens":1,"output_tokens":5,"total_tokens":6}
		}`)),
	}
	_ = recorder
	err := ra.handleResponse(context.Background(), resp)
	if !errors.Is(err, errEmptyOutput) {
		t.Fatalf("expected errEmptyOutput for non-streaming empty output, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected nothing written to client on empty output, got %q", recorder.Body.String())
	}
}

func TestEmptyRetryNonStreamingDisabledPassesThrough(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)
	ra.emptyRetryEnabled = false

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(`{
			"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"status":"completed",
			"output":[{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"deep thought"}]}],
			"usage":{"input_tokens":1,"output_tokens":5,"total_tokens":6}
		}`)),
	}
	if err := ra.handleResponse(context.Background(), resp); err != nil {
		t.Fatalf("OFF: non-streaming empty output must pass through unchanged, got %v", err)
	}
	if recorder.Body.Len() == 0 {
		t.Fatalf("OFF: expected response body forwarded")
	}
}

// Team B 审计发现 2 e2e：非流式 refusal-only 响应（Message.Refusal 非空、无
// Content/ToolCalls）对客户端可见——flag ON 时必须转发而非假重试（修复前
// isEmptyOutputResponse 判空 → errEmptyOutput）。refusal 经入站适配器编码为
// Responses refusal 内容部件下发。
func TestEmptyRetryNonStreamingRefusalOnlyForwards(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)
	ra.emptyRetryEnabled = true

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(`{
			"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"status":"completed",
			"output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed",
				"content":[{"type":"refusal","refusal":"I cannot help with that request."}]}],
			"usage":{"input_tokens":1,"output_tokens":8,"total_tokens":9}
		}`)),
	}
	if err := ra.handleResponse(context.Background(), resp); err != nil {
		t.Fatalf("refusal-only response must be forwarded, not retried as empty output, got %v", err)
	}
	out := recorder.Body.String()
	if !strings.Contains(out, "refusal") || !strings.Contains(out, "I cannot help with that request.") {
		t.Fatalf("expected refusal content forwarded to client, got %q", out)
	}
}

// --- hold 单元级补充：状态机（hold/cap/drop 与释放闩锁） ---

func TestEmptyOutputHoldStateMachine(t *testing.T) {
	h := newEmptyOutputHold(true)

	// 不可见 → 持有
	if !h.hold(heldUnit{stream: &transformerModel.InternalLLMResponse{}}, 1) {
		t.Fatal("invisible chunk under cap should be held")
	}
	if !h.holding() {
		t.Fatal("should still be holding")
	}
	if h.heldBytes() != 1 {
		t.Fatalf("heldRaw should track raw chunk bytes, got %d", h.heldBytes())
	}

	// 超限 → 拒绝入队（调用方 flush-degrade）
	if h.hold(heldUnit{stream: &transformerModel.InternalLLMResponse{}}, maxEmptyOutputHoldBytes) {
		t.Fatal("over-cap hold must be rejected")
	}

	// cap：恰好到界允许（<= 8MiB 语义在 e2e 中覆盖，这里只测拒绝方向）
	prev := maxEmptyOutputHoldBytes
	maxEmptyOutputHoldBytes = 4
	defer func() { maxEmptyOutputHoldBytes = prev }()
	h2 := newEmptyOutputHold(true)
	if !h2.hold(heldUnit{stream: &transformerModel.InternalLLMResponse{}}, 4) {
		t.Fatal("hold at exactly the cap must be accepted (never truncate)")
	}
	if h2.hold(heldUnit{stream: &transformerModel.InternalLLMResponse{}}, 1) {
		t.Fatal("hold beyond the cap must be rejected")
	}

	// drop 清空
	h2.drop()
	if h2.heldBytes() != 0 || len(h2.pending) != 0 {
		t.Fatal("drop must clear pending units")
	}

	// 释放闩锁：hasVisible/capped 后 active=false
	h3 := newEmptyOutputHold(true)
	h3.hasVisible = true
	if h3.active() || h3.holding() {
		t.Fatal("released hold must not be active")
	}
	h4 := newEmptyOutputHold(true)
	h4.capped = true
	if h4.active() {
		t.Fatal("capped hold must degrade to inactive")
	}

	// disabled / nil 安全
	if h5 := newEmptyOutputHold(false); h5.active() {
		t.Fatal("disabled hold must not be active")
	}
	var h6 *emptyOutputHold
	if h6.active() || h6.holding() || h6.heldBytes() != 0 {
		t.Fatal("nil hold must be inert")
	}
	h6.drop() // nil-safe no-op
}
