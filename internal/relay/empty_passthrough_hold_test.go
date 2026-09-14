package relay

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/relay/stream"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/bestruirui/octopus/internal/utils/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// G6 passthrough hold-until-output-evidence 的端到端行为测试（实验开关，默认 OFF）。

func enablePassthroughHoldForTest(t *testing.T) {
	t.Helper()
	prev := emptyPassthroughHoldEnabled
	emptyPassthroughHoldEnabled = func() bool { return true }
	t.Cleanup(func() { emptyPassthroughHoldEnabled = prev })
}

// 事故签名流：created → completed 空壳（零输出事件、零 usage）。hold ON 时全程保持，
// 流尾判 failure —— 客户端零字节、ErrEmptyUpstreamStream（同通道重试）。
func TestPassthroughHoldShellStreamRetries(t *testing.T) {
	enablePassthroughHoldForTest(t)
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_shell","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_shell","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"completed"}}`,
		"",
		"",
	}, "\n")

	err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), ra.ptCfg())
	if !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("expected ErrEmptyUpstreamStream for shell stream under hold, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected zero bytes to client (held, fresh clean stream on retry), got %q", recorder.Body.String())
	}
}

// 健康流：created → 输出事件 → completed(usage>0)。首个输出事件即 flush 全部，
// 之后纯直通——客户端按序收到 created/delta/completed。
func TestPassthroughHoldHealthyStreamForwardsAll(t *testing.T) {
	enablePassthroughHoldForTest(t)
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_ok","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"hello"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_ok","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"completed","usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}`,
		"",
		"",
	}, "\n")

	if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), ra.ptCfg()); err != nil {
		t.Fatalf("expected healthy stream to forward, got %v", err)
	}
	out := recorder.Body.String()
	for _, want := range []string{`"response.created"`, `"hello"`, `"response.completed"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %s in output, got %q", want, out)
		}
	}
	if strings.Index(out, `"response.created"`) > strings.Index(out, `"hello"`) {
		t.Fatalf("expected held created flushed before delta, got %q", out)
	}
}

// 合法空轮：completed 且 usage.output>0（#40200）——终态判 healthy，放行。
func TestPassthroughHoldLegitEmptyTurnForwards(t *testing.T) {
	enablePassthroughHoldForTest(t)
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_legit","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_legit","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"completed","usage":{"input_tokens":10,"output_tokens":3,"total_tokens":13}}}`,
		"",
		"",
	}, "\n")

	if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), ra.ptCfg()); err != nil {
		t.Fatalf("expected legit empty turn (output>0) to forward, got %v", err)
	}
	if !strings.Contains(recorder.Body.String(), `"response.completed"`) {
		t.Fatalf("expected completed forwarded, got %q", recorder.Body.String())
	}
}

// 8 MiB 超限：flush-degrade 直通（不截断）。
func TestPassthroughHoldCapDegradesToPassthrough(t *testing.T) {
	enablePassthroughHoldForTest(t)
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	prev := maxEmptyOutputHoldBytes
	maxEmptyOutputHoldBytes = 64
	defer func() { maxEmptyOutputHoldBytes = prev }()

	big := strings.Repeat("r", 200)
	// 大体积 void-prefix 不可能（created/in_progress 很小）——用未类型化大块触发
	// 保守放行路径之外的 cap 检查：observe 对未类型化空 type 先保守放行，因此
	// 用 created 后跟超长 in_progress 风格块不现实。直接对 hold 状态机注入：
	hold := newPassthroughOutputHold(responsesTestConfig())
	if !hold.hold([]byte(strings.Repeat("x", 50))) {
		t.Fatal("small chunk should be held")
	}
	if hold.hold([]byte(big)) {
		t.Fatal("expected cap rejection")
	}
	_ = big
	_ = recorder
	_ = ra
}

// OFF 等价：开关关闭时纯直通（现有 characterization 不变，双确认）。
func TestPassthroughHoldOffEquivalent(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_off","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_off","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"completed"}}`,
		"",
		"",
	}, "\n")

	// OFF（默认）：shell 流 verbatim 转发 + success 记账（3c32c00e 表征）。
	if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), ra.ptCfg()); err != nil {
		t.Fatalf("OFF must keep nil return (verbatim passthrough), got %v", err)
	}
	if recorder.Body.Len() == 0 {
		t.Fatal("OFF must forward shell payload verbatim")
	}
}

// ptCfg 从适配器取 passthrough 配置（避免测试文件重复构造）。
func (ra *relayAttempt) ptCfg() transformerModel.PassthroughConfig {
	pt := ra.outAdapter.(transformerModel.PassthroughCapable)
	return pt.PassthroughConfig()
}

// 判别器单元表：observe 决策矩阵。
func TestPassthroughHoldObserveMatrix(t *testing.T) {
	h := newPassthroughOutputHold(responsesTestConfig())

	cases := []struct {
		name     string
		typ      string
		data     string
		expected passthroughHoldAction
	}{
		{"void_prefix_kept", "response.created", `{}`, passthroughHoldKeep},
		{"in_progress_kept", "response.in_progress", `{}`, passthroughHoldKeep},
		{"terminal_no_usage_suspect", "response.completed", `{"type":"response.completed","response":{"status":"completed"}}`, passthroughHoldSuspect},
		{"terminal_zero_output_suspect", "response.completed", `{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":5,"output_tokens":0}}}`, passthroughHoldSuspect},
		{"terminal_positive_output_release", "response.completed", `{"type":"response.completed","response":{"status":"completed","usage":{"output_tokens":4}}}`, passthroughHoldRelease},
		{"error_event_release", "response.failed", `{"type":"response.failed"}`, passthroughHoldRelease},
		{"output_event_release", "response.output_text.delta", `{"type":"response.output_text.delta"}`, passthroughHoldRelease},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h2 := newPassthroughOutputHold(responsesTestConfig())
			if got := h2.observe(tc.typ, []byte(tc.data)); got != tc.expected {
				t.Fatalf("expected action %d, got %d", tc.expected, got)
			}
		})
	}

	// 批次二②翻转后的两类未类型化帧：
	// - 注释帧（无 data 行，SSE 规范忽略语义）→ Keep；
	// - malformed data 帧（data 行但 JSON 不可解析）→ 保守放行不变。
	if got := h.observe("", []byte(": ping - 1726300000\n\n")); got != passthroughHoldKeep {
		t.Fatalf("comment frame must keep (SSE ignore semantics), got %d", got)
	}
	if got := h.observe("", []byte("data: not json\n\n")); got != passthroughHoldRelease {
		t.Fatalf("malformed data frame must release conservatively, got %d", got)
	}
}

// responsesTestConfig 构造与 openai.ResponseOutbound 等价的 Responses 分类法
// （测试不依赖适配器注册表时使用）。
func responsesTestConfig() transformerModel.PassthroughConfig {
	return transformerModel.PassthroughConfig{
		TerminalEvents: map[string]struct{}{
			"response.completed":  {},
			"response.failed":     {},
			"response.incomplete": {},
			"error":               {},
		},
		ErrorEvents:      map[string]struct{}{"response.failed": {}, "error": {}},
		VoidPrefixEvents: map[string]struct{}{"response.created": {}, "response.in_progress": {}},
	}
}

// round-5 开灯前置①的验收测试：被 hold 的壳流在 OnFinish 饿死路径上仍必须产出
// 缺陷族告警与 usage 形态影子行（取证链闭合）；对照组是 void-prefix 后中途截断
// 的流（EOF、无终态帧）——不入 usage 桶（client_gone 免费标签规则：截断 ≠ 完成的空）。
func TestPassthroughHoldShellStreamShadowEmittedPostRun(t *testing.T) {
	enablePassthroughHoldForTest(t)
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	observedCore, observed := observer.New(zapcore.InfoLevel)
	prevLogger := log.Logger
	log.Logger = zap.New(observedCore).Sugar()
	defer func() { log.Logger = prevLogger }()

	// 事故签名流：created → completed 空壳（零输出事件、零 usage、契约内 breach）。
	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_shell","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_shell","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"completed"}}`,
		"",
		"",
	}, "\n")

	err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), ra.ptCfg())
	if !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("expected ErrEmptyUpstreamStream for shell stream under hold, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected zero bytes to client (held), got %q", recorder.Body.String())
	}

	// OnFinish 早退路径上的取证回填：缺陷族告警 + 影子行都必须在场。
	warns := observed.FilterMessage("relay.empty_stream").All()
	if len(warns) != 1 {
		t.Fatalf("expected exactly one relay.empty_stream warn post-run, got %d", len(warns))
	}
	fields := map[string]string{}
	for _, f := range warns[0].Context {
		fields[f.Key] = f.String
	}
	if fields["empty_stream_kind"] != "terminal_no_output" {
		t.Fatalf("expected empty_stream_kind=terminal_no_output, got %q (fields: %v)", fields["empty_stream_kind"], fields)
	}
	if fields["stream_end_reason"] != "empty" {
		t.Fatalf("expected stream_end_reason=empty, got %q (fields: %v)", fields["stream_end_reason"], fields)
	}
	shadows := observed.FilterMessage("relay.empty_stream_shadow").All()
	if len(shadows) != 1 {
		t.Fatalf("expected exactly one relay.empty_stream_shadow info post-run, got %d", len(shadows))
	}
	sfields := map[string]string{}
	for _, f := range shadows[0].Context {
		sfields[f.Key] = f.String
	}
	if sfields["usage_form"] != "usage_absent" {
		t.Fatalf("expected usage_form=usage_absent, got %q (fields: %v)", sfields["usage_form"], sfields)
	}
}

// 对照组：仅 void-prefix（created）后上游 EOF——hold 保持但无 Suspect 终态帧，
// 不得产出 relay.empty_stream / relay.empty_stream_shadow（截断 ≠ 完成的空）。
func TestPassthroughHoldTruncatedStreamStaysOutOfUsageBucket(t *testing.T) {
	enablePassthroughHoldForTest(t)
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	observedCore, observed := observer.New(zapcore.InfoLevel)
	prevLogger := log.Logger
	log.Logger = zap.New(observedCore).Sugar()
	defer func() { log.Logger = prevLogger }()

	// created 后立即 EOF（无终态帧）。
	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_trunc","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"in_progress"}}`,
		"",
		"",
	}, "\n")

	err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), ra.ptCfg())
	if !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("expected ErrEmptyUpstreamStream for truncated stream, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected zero bytes to client (held), got %q", recorder.Body.String())
	}

	if warns := observed.FilterMessage("relay.empty_stream").All(); len(warns) != 0 {
		t.Fatalf("truncated stream must not emit relay.empty_stream, got %d", len(warns))
	}
	if shadows := observed.FilterMessage("relay.empty_stream_shadow").All(); len(shadows) != 0 {
		t.Fatalf("truncated stream must not emit relay.empty_stream_shadow, got %d", len(shadows))
	}
}

// 闩锁单元：observe 的 Suspect 分支置位 sawSuspect；Keep 分支不置位。
// 字节经 transform 流转（observe 单元直调不经过 hold(frames)，buf 恒空）。
func TestPassthroughHoldSuspectLatch(t *testing.T) {
	h := newPassthroughOutputHold(responsesTestConfig())
	if _, err := h.transform([]byte("data: {\"type\":\"response.created\"}\n\n")); err != nil {
		t.Fatalf("transform keep chunk failed: %v", err)
	}
	if h.suspect_() {
		t.Fatal("keep branch must not set sawSuspect latch")
	}
	if got := h.observe("response.created", []byte(`{}`)); got != passthroughHoldKeep {
		t.Fatalf("void-prefix must keep, got %d", got)
	}
	if _, err := h.transform([]byte("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n")); err != nil {
		t.Fatalf("transform suspect chunk failed: %v", err)
	}
	if !h.suspect_() {
		t.Fatal("suspect branch must set sawSuspect latch")
	}
	if !h.holding_() {
		t.Fatal("suspect must keep holding")
	}
	// heldRaw 快照：buf 与 pending 之和（只读，不改变保持状态）。
	if len(h.heldRaw()) == 0 {
		t.Fatal("heldRaw must return held bytes snapshot")
	}
}

// ---------- 跨 chunk 帧组装表征测试（round-5 开灯前置④）----------
//
// RawSource 定长 32KB 读：真实流中一个 chunk 可含 N 个完整帧 + 半个尾帧。
// 现有 e2e 测试体恒单 chunk,组装路径(pending 半帧尾缓存、带半帧尾的 Release
// flush、cap-degrade 端到端)覆盖为零。本节锁定现状语义——包括未类型化保守
// 放行——为批次二 Keep 翻转垫底。全部输入经 transform 真实路径,不注入内部状态。

// created 帧拆两 chunk:半帧尾挂 pending,下 chunk 补全 → 全部 keep,零字节放行。
func TestPassthroughHoldSplitVoidPrefixAcrossChunks(t *testing.T) {
	h := newPassthroughOutputHold(responsesTestConfig())

	full := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"status\":\"in_progress\"}}\n\n"
	split := strings.Index(full, "\"status\"")

	out1, err := h.transform([]byte(full[:split]))
	if err != nil {
		t.Fatalf("partial created chunk failed: %v", err)
	}
	if len(out1) != 0 {
		t.Fatalf("partial void-prefix must be held (nil out), got %q", out1)
	}
	if !h.holding_() {
		t.Fatal("hold must survive a pending half-frame")
	}

	out2, err := h.transform([]byte(full[split:]))
	if err != nil {
		t.Fatalf("completed half of created frame failed: %v", err)
	}
	if len(out2) != 0 {
		t.Fatalf("void-prefix completion must stay held, got %q", out2)
	}
	if !h.holding_() {
		t.Fatal("void-prefix only must keep holding")
	}
}

// suspect 终态拆 chunk:补全后 observe 命中 Suspect,闩锁与保持语义不因拆帧漂移。
func TestPassthroughHoldSplitSuspectTerminalAcrossChunks(t *testing.T) {
	h := newPassthroughOutputHold(responsesTestConfig())

	if _, err := h.transform([]byte("data: {\"type\":\"response.created\"}\n\n")); err != nil {
		t.Fatalf("created chunk failed: %v", err)
	}
	if h.suspect_() {
		t.Fatal("latch must stay clear before terminal frame")
	}

	full := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\"}}\n\n"
	split := strings.Index(full, "\"status\"")

	if _, err := h.transform([]byte(full[:split])); err != nil {
		t.Fatalf("partial terminal chunk failed: %v", err)
	}
	if h.suspect_() {
		t.Fatal("half frame must not be observed yet")
	}

	if _, err := h.transform([]byte(full[split:])); err != nil {
		t.Fatalf("terminal completion failed: %v", err)
	}
	if !h.suspect_() {
		t.Fatal("completed terminal frame must set suspect latch")
	}
	if !h.holding_() {
		t.Fatal("suspect terminal must keep holding to stream end")
	}
}

// keep 帧后输出事件到达:flush 载荷按原始字节序包含 created 与 delta,
// 此后永久直通(再喂 chunk 原样透传)。
func TestPassthroughHoldReleaseFlushKeepsOriginalOrder(t *testing.T) {
	h := newPassthroughOutputHold(responsesTestConfig())

	if _, err := h.transform([]byte("data: {\"type\":\"response.created\"}\n\n")); err != nil {
		t.Fatalf("created chunk failed: %v", err)
	}

	delta := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n"
	out, err := h.transform([]byte(delta))
	if err != nil {
		t.Fatalf("delta chunk failed: %v", err)
	}
	outStr := string(out)
	if !strings.Contains(outStr, "\"response.created\"") || !strings.Contains(outStr, "hello") {
		t.Fatalf("flush payload must contain held created + delta, got %q", outStr)
	}
	if strings.Index(outStr, "\"response.created\"") > strings.Index(outStr, "hello") {
		t.Fatalf("held bytes must precede releasing chunk, got %q", outStr)
	}
	if h.holding_() {
		t.Fatal("release must end holding")
	}

	// 永久直通:后续 chunk 原样透传。
	tail := "data: {\"type\":\"response.output_text.done\"}\n\n"
	out2, err := h.transform([]byte(tail))
	if err != nil {
		t.Fatalf("post-release chunk failed: %v", err)
	}
	if string(out2) != tail {
		t.Fatalf("post-release passthrough must be verbatim, got %q", out2)
	}
}

// Release 时 chunk 携带半帧尾:flush 含持有字节与完整 chunk(半帧尾随行),
// 下一 chunk 直通补全——流顺序仍由字节序保证。
func TestPassthroughHoldReleaseWithPendingTail(t *testing.T) {
	h := newPassthroughOutputHold(responsesTestConfig())

	if _, err := h.transform([]byte("data: {\"type\":\"response.created\"}\n\n")); err != nil {
		t.Fatalf("created chunk failed: %v", err)
	}

	// delta 完整帧 + 下一帧的开头(半帧尾)。
	deltaPlus := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\ndata: {\"type\":\"response.output"
	out, err := h.transform([]byte(deltaPlus))
	if err != nil {
		t.Fatalf("delta+partial chunk failed: %v", err)
	}
	// flush 契约:持有字节(created)+ 本 chunk 全部(含半帧尾)按原始顺序一次写出。
	created := "data: {\"type\":\"response.created\"}\n\n"
	if string(out) != created+deltaPlus {
		t.Fatalf("release flush must emit held bytes + full chunk verbatim (incl. pending tail), got %q", out)
	}

	// 半帧尾的后续:已直通,原样透传。
	rest := "_done\"}\n\n"
	out2, err := h.transform([]byte(rest))
	if err != nil {
		t.Fatalf("tail completion failed: %v", err)
	}
	if string(out2) != rest {
		t.Fatalf("post-release chunk must pass through, got %q", out2)
	}
}

// cap-degrade 端到端:小 maxEmptyOutputHoldBytes 下,持有字节+当前 chunk 完整
// 写出(绝不截断),保持状态解除。
func TestPassthroughHoldCapDegradeEndToEnd(t *testing.T) {
	prev := maxEmptyOutputHoldBytes
	maxEmptyOutputHoldBytes = 48
	defer func() { maxEmptyOutputHoldBytes = prev }()

	h := newPassthroughOutputHold(responsesTestConfig())

	if _, err := h.transform([]byte("data: {\"type\":\"response.created\"}\n\n")); err != nil {
		t.Fatalf("created chunk failed: %v", err)
	}
	if !h.holding_() {
		t.Fatal("small created chunk fits under cap and stays held")
	}

	// 大块输出:hold 拒绝 → flush-degrade,完整写出。
	big := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"" + strings.Repeat("r", 80) + "\"}\n\n"
	out, err := h.transform([]byte(big))
	if err != nil {
		t.Fatalf("big chunk failed: %v", err)
	}
	outStr := string(out)
	if !strings.Contains(outStr, "\"response.created\"") || !strings.Contains(outStr, strings.Repeat("r", 80)) {
		t.Fatalf("flush-degrade must emit held + current chunk complete, got %d bytes", len(outStr))
	}
	if h.holding_() {
		t.Fatal("cap degrade must release hold")
	}
	if strings.Count(outStr, "response.created") != 1 {
		t.Fatalf("no byte may be duplicated or truncated under degrade, got %q", outStr)
	}
}

// 批次二②翻转后的未类型化帧处置:注释帧(无 data 行,SSE 规范的忽略语义)→
// Keep(持有继续,不触发永久直通);malformed data 帧 → 保守放行(真含糊输入
// 不演变成静默截断)。
func TestPassthroughHoldUntypedChunkConservativeRelease(t *testing.T) {
	h := newPassthroughOutputHold(responsesTestConfig())

	if _, err := h.transform([]byte("data: {\"type\":\"response.created\"}\n\n")); err != nil {
		t.Fatalf("created chunk failed: %v", err)
	}

	// 注释帧(`: ping`)无事件语义:Keep,持有字节继续持有,零放行。
	comment := ": ping - 1726300000\n\n"
	out, err := h.transform([]byte(comment))
	if err != nil {
		t.Fatalf("comment chunk failed: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("comment frame must stay held, got %q", out)
	}
	if !h.holding_() {
		t.Fatal("comment frame must not end holding")
	}

	// malformed data 帧仍保守放行:持有字节 + 本 chunk 完整 flush(绝不截断)。
	badData := "data: not json\n\n"
	out2, err := h.transform([]byte(badData))
	if err != nil {
		t.Fatalf("malformed data chunk failed: %v", err)
	}
	created := "data: {\"type\":\"response.created\"}\n\n"
	if string(out2) != created+comment+badData {
		t.Fatalf("conservative release must flush held bytes + comment + malformed data in order, got %q", out2)
	}
	if h.holding_() {
		t.Fatal("conservative release must end holding")
	}

	// 对照:纯 JSON 类型化路径不受影响。
	h2 := newPassthroughOutputHold(responsesTestConfig())
	if got := h2.observe("response.created", []byte(`{}`)); got != passthroughHoldKeep {
		t.Fatalf("typed void-prefix must keep, got %d", got)
	}
}

// ---------- 灯下演练（窗口零）的代码级前置锁定 ----------
//
// 规程(ops-protocol 附录)要求演练验证三件事:开关不重启即生效、拼错值大声拒绝、
// 在飞流不受中途翻转影响。三者都是可测的代码契约,先在此锁定,演练时只验证
// 运维路径(API 调用、日志检索)。

// 前置 1+2:开关动态生效——同一进程内,默认 OFF 与显式 OFF 等价,显式 ON 立即
// 拦截壳流;此后翻回 OFF,新流立即恢复直通。
func TestPassthroughHoldSwitchTakesEffectWithoutRestart(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_dyn","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_dyn","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"completed"}}`,
		"",
		"",
	}, "\n")

	// OFF（默认）:直通。
	if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), ra.ptCfg()); err != nil {
		t.Fatalf("OFF baseline must forward, got %v", err)
	}
	if recorder.Body.Len() == 0 {
		t.Fatal("OFF baseline must forward shell payload")
	}

	// ON:立即生效,壳流保持,零字节。
	enablePassthroughHoldForTest(t)
	recorder.Body.Reset()
	if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), ra.ptCfg()); !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("ON must intercept shell stream immediately, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("ON must hold shell stream (zero bytes), got %q", recorder.Body.String())
	}

	// 翻回 OFF:新流立即恢复直通。
	prev := emptyPassthroughHoldEnabled
	emptyPassthroughHoldEnabled = func() bool { return false }
	recorder.Body.Reset()
	if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), ra.ptCfg()); err != nil {
		t.Fatalf("flip-back OFF must forward again, got %v", err)
	}
	if recorder.Body.Len() == 0 {
		t.Fatal("flip-back OFF must forward shell payload again")
	}
	_ = prev
}

// 前置 3:在飞流不受中途翻转影响——开关在流建立时读一次(闭包持有 ptHold),
// 流中途把开关翻 OFF 不改变本流的保持行为,壳流仍走到流尾终判。
func TestPassthroughHoldInFlightStreamUnaffectedByMidStreamFlip(t *testing.T) {
	enablePassthroughHoldForTest(t)
	ra, recorder := newEmptyStreamTestAttempt(t, inboundOpenAIResponse(), transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_inflight","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_inflight","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"completed"}}`,
		"",
		"",
	}, "\n")

	// 流中途翻 OFF:开关是每流只读一次的变量(relay.go:1309 建立点),用
	// "首次读返回 ON、此后一直 OFF"模拟翻转时序——建立读走 ON 后翻转即刻发生。
	// 若实现中途重读(会拿到 OFF 并放行壳流),本流与零字节断言都会失败;
	// 末尾同时锁定"每流恰读一次"契约。
	reads := 0
	prev := emptyPassthroughHoldEnabled
	emptyPassthroughHoldEnabled = func() bool {
		reads++
		return reads == 1
	}
	defer func() { emptyPassthroughHoldEnabled = prev }()

	if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), ra.ptCfg()); !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("in-flight stream must keep the build-time ON decision, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("in-flight stream must stay held (zero bytes), got %q", recorder.Body.String())
	}
	if reads != 1 {
		t.Fatalf("switch must be read exactly once per stream (at establishment), got %d reads", reads)
	}
}

// splitSSEFrames 纯字节分帧的单元表征(单向收敛):\n\n 与 \r\n\r\n 混合边界、
// 多帧单 chunk、半帧尾——无语义解释,锁定后供批次二统一。
func TestSplitSSEFramesByteFraming(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantN     int
		wantRest  string
		wantFirst string
	}{
		{
			name:      "single frame",
			input:     "data: {}\n\n",
			wantN:     1,
			wantRest:  "",
			wantFirst: "data: {}\n\n",
		},
		{
			name:     "two frames one chunk",
			input:    "data: {a}\n\ndata: {b}\n\n",
			wantN:    2,
			wantRest: "",
		},
		{
			name:      "partial tail frame",
			input:     "data: {a}\n\ndata: {b",
			wantN:     1,
			wantRest:  "data: {b",
			wantFirst: "data: {a}\n\n",
		},
		{
			name:      "crlf boundary",
			input:     "data: {a}\r\n\r\n",
			wantN:     1,
			wantRest:  "",
			wantFirst: "data: {a}\r\n\r\n",
		},
		{
			name:     "mixed boundaries",
			input:    "data: {a}\n\ndata: {b}\r\n\r\n",
			wantN:    2,
			wantRest: "",
		},
		{
			name:     "only partial frame",
			input:    "data: {a",
			wantN:    0,
			wantRest: "data: {a",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frames, rest := splitSSEFrames([]byte(tc.input))
			if len(frames) != tc.wantN {
				t.Fatalf("got %d frames, want %d (frames: %q)", len(frames), tc.wantN, frames)
			}
			if string(rest) != tc.wantRest {
				t.Fatalf("rest = %q, want %q", rest, tc.wantRest)
			}
			if tc.wantFirst != "" && (len(frames) == 0 || string(frames[0]) != tc.wantFirst) {
				t.Fatalf("first frame = %q, want %q", frames, tc.wantFirst)
			}
			// 字节序不变性:帧 + 尾拼接必须还原输入(无字节丢失/重排)。
			var rebuilt []byte
			for _, f := range frames {
				rebuilt = append(rebuilt, f...)
			}
			rebuilt = append(rebuilt, rest...)
			if string(rebuilt) != tc.input {
				t.Fatalf("round-trip mismatch: got %q, want %q", rebuilt, tc.input)
			}
		})
	}
}
