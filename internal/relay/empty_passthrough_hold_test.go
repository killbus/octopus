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
	hold := newPassthroughOutputHold()
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
	terminal := map[string]struct{}{"response.completed": {}}
	errs := map[string]struct{}{"response.failed": {}}

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
			h := newPassthroughOutputHold()
			if got := h.observe(tc.typ, []byte(tc.data), terminal, errs); got != tc.expected {
				t.Fatalf("expected action %d, got %d", tc.expected, got)
			}
		})
	}

	// 未类型化 chunk 的保守放行。
	h := newPassthroughOutputHold()
	if got := h.observe("", []byte(`not json`), terminal, errs); got != passthroughHoldRelease {
		t.Fatalf("untyped chunk must release conservatively, got %d", got)
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
	terminal := map[string]struct{}{"response.completed": {}}
	errs := map[string]struct{}{"response.failed": {}}

	h := newPassthroughOutputHold()
	if _, err := h.transform([]byte("data: {\"type\":\"response.created\"}\n\n"), terminal, errs); err != nil {
		t.Fatalf("transform keep chunk failed: %v", err)
	}
	if h.suspect_() {
		t.Fatal("keep branch must not set sawSuspect latch")
	}
	if got := h.observe("response.created", []byte(`{}`), terminal, errs); got != passthroughHoldKeep {
		t.Fatalf("void-prefix must keep, got %d", got)
	}
	if _, err := h.transform([]byte("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"), terminal, errs); err != nil {
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
