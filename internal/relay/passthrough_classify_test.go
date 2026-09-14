package relay

import (
	"context"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/inbound"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

// classifyPassthroughStreamEnd 将直通缓存流的结尾归为五类，是 relay.empty_stream
// 观测日志的判定核心。此处逐类固化其语义，尤其是“error_event 优先于 terminal”
// 与“解析失败不猜测”两条裁定（research/empty-output-retry-audit.md R4）。

func TestClassifyPassthroughStreamEnd(t *testing.T) {
	responsesTerminal := map[string]struct{}{
		"response.completed":  {},
		"response.failed":     {},
		"response.incomplete": {},
		"error":               {},
	}
	responsesError := map[string]struct{}{
		"response.failed": {},
		"error":           {},
	}
	anthropicTerminal := map[string]struct{}{
		"message_stop": {},
		"error":        {},
	}
	anthropicError := map[string]struct{}{
		"error": {},
	}
	responsesVoidPrefix := map[string]struct{}{
		"response.created":     {},
		"response.in_progress": {},
	}
	anthropicVoidPrefix := map[string]struct{}{
		"message_start": {},
		"ping":          {},
	}

	created := `data: {"type":"response.created","response":{"id":"resp_1","status":"in_progress"}}` + "\n\n"
	completed := `data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}` + "\n\n"

	cases := []struct {
		name             string
		rawStream        string
		terminalEvents   map[string]struct{}
		errorEvents      map[string]struct{}
		voidPrefixEvents map[string]struct{}
		want             string
	}{
		{
			name:           "empty raw stream",
			rawStream:      "",
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "empty",
		},
		{
			name:           "comment-only stream has zero events",
			rawStream:      ":\n\n:\n\n",
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "empty",
		},
		{
			name:           "openai response.failed event type",
			rawStream:      created + `data: {"type":"response.failed","response":{"error":{"code":429,"message":"rate limited"}}}` + "\n\n",
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "error_event",
		},
		{
			name:           "anthropic SSE event field error",
			rawStream:      "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n",
			terminalEvents: anthropicTerminal, errorEvents: anthropicError, voidPrefixEvents: anthropicVoidPrefix,
			want: "error_event",
		},
		{
			name:           "untyped top-level error payload",
			rawStream:      `data: {"error":{"message":"Azure 429","type":"rate_limit"}}` + "\n\n",
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "error_event",
		},
		{
			name:           "explicit null error payload is not an error",
			rawStream:      created + `data: {"type":"response.output_text.delta","error":null,"delta":"hi"}` + "\n\n",
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "unclassified",
		},
		{
			name:           "terminal completed event",
			rawStream:      created + completed,
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "terminal",
		},
		{
			name:           "anthropic message_stop terminal",
			rawStream:      "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
			terminalEvents: anthropicTerminal, errorEvents: anthropicError, voidPrefixEvents: anthropicVoidPrefix,
			want: "terminal",
		},
		{
			name:           "error event after terminal wins",
			rawStream:      completed + `data: {"error":{"message":"late failure"}}` + "\n\n",
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "error_event",
		},
		{
			name:           "truncated stream cut mid-event",
			rawStream:      created + `data: {"type":"response.output_te`,
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "truncated",
		},
		{
			name:           "parse failure after terminal still terminal",
			rawStream:      created + completed + `data: {"garba`,
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "terminal",
		},
		{
			name:           "parse failure with zero events is unclassified",
			rawStream:      `data: {"garbage-without-newline`,
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "unclassified",
		},
		{
			name:           "non-matching events with clean EOF",
			rawStream:      created + `data: {"type":"response.queue_position","position":1}` + "\n\n",
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "unclassified",
		},
		{
			name:           "multi-line data field joins into one JSON payload",
			rawStream:      "data: {\"type\":\ndata: \"response.completed\",\"response\":{}}\n\n",
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "terminal",
		},
		{
			name:           "JSON without type field is not an error",
			rawStream:      `data: {"foo":"bar","error_hint":"no such key"}` + "\n\n",
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "unclassified",
		},
		{
			name:           "non-JSON payload containing error substring is not an error",
			rawStream:      `data: plain text with the word error inside` + "\n\n",
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			want: "unclassified",
		},
		{
			name:           "nil sets never panic",
			rawStream:      created + completed,
			terminalEvents: nil, errorEvents: nil, voidPrefixEvents: nil,
			want: "unclassified",
		},
		{
			name:           "nil sets still detect error via payload shape",
			rawStream:      created + `data: {"error":{"message":"x"}}` + "\n\n",
			terminalEvents: nil, errorEvents: nil, voidPrefixEvents: nil,
			want: "error_event",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyPassthroughStreamEnd([]byte(tc.rawStream), tc.terminalEvents, tc.errorEvents, tc.voidPrefixEvents)
			if got != tc.want {
				t.Fatalf("classifyPassthroughStreamEnd() = %q, want %q", got, tc.want)
			}
		})
	}
}

// 输出证据维度：终态只证明流未中断，不证明生成发生过。输出事件 = 类型不在
// terminal/error 集合里的非元事件（信封层协议形状检查，非内容启发式）。
func TestClassifyPassthroughStreamEndOutputEvidence(t *testing.T) {
	responsesTerminal := map[string]struct{}{
		"response.completed":  {},
		"response.failed":     {},
		"response.incomplete": {},
		"error":               {},
	}
	responsesError := map[string]struct{}{
		"response.failed": {},
		"error":           {},
	}

	created := `data: {"type":"response.created","response":{"id":"resp_1","status":"in_progress"}}` + "\n\n"
	inProgress := `data: {"type":"response.in_progress","response":{"id":"resp_1","status":"in_progress"}}` + "\n\n"
	completed := `data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}` + "\n\n"

	responsesVoidPrefix := map[string]struct{}{
		"response.created":     {},
		"response.in_progress": {},
	}

	cases := []struct {
		name             string
		rawStream        string
		terminalEvents   map[string]struct{}
		errorEvents      map[string]struct{}
		voidPrefixEvents map[string]struct{}
		wantKind         string
		wantEvidence     bool
	}{
		{
			// 生产事故签名（LiteLLM 桥把 429 洗成 200 空 completed 流）：
			// 信封俱全、零输出事件。empty-output-retry-audit.md 第三轮焦点。
			name:           "created then completed with zero output events",
			rawStream:      created + completed,
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			wantKind: "terminal", wantEvidence: false,
		},
		{
			name:           "in_progress only before terminal counts no evidence",
			rawStream:      created + inProgress + completed,
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			wantKind: "terminal", wantEvidence: false,
		},
		{
			name:           "reasoning summary delta is output evidence",
			rawStream:      created + `data: {"type":"response.reasoning_summary_text.delta","delta":"thinking"}` + "\n\n" + completed,
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			wantKind: "terminal", wantEvidence: true,
		},
		{
			name:           "output_item.added alone is output evidence",
			rawStream:      created + `data: {"type":"response.output_item.added","output_index":0,"item":{"type":"message","role":"assistant"}}` + "\n\n" + completed,
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			wantKind: "terminal", wantEvidence: true,
		},
		{
			name:           "text delta is output evidence",
			rawStream:      created + `data: {"type":"response.output_text.delta","delta":"hi"}` + "\n\n" + completed,
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			wantKind: "terminal", wantEvidence: true,
		},
		{
			name:           "untyped events count as evidence",
			rawStream:      created + `data: {"type":"response.queue_position","position":1}` + "\n\n" + completed,
			terminalEvents: responsesTerminal, errorEvents: responsesError, voidPrefixEvents: responsesVoidPrefix,
			wantKind: "terminal", wantEvidence: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kind, evidence := classifyPassthroughStreamEndWithEvidence([]byte(tc.rawStream), tc.terminalEvents, tc.errorEvents, tc.voidPrefixEvents)
			if kind != tc.wantKind {
				t.Fatalf("kind = %q, want %q", kind, tc.wantKind)
			}
			if evidence != tc.wantEvidence {
				t.Fatalf("evidence = %v, want %v", evidence, tc.wantEvidence)
			}
		})
	}
}

// 事件超过 sse.Read MaxEventSize（bufio.ErrTooLong）时按解析失败处理。maxSSEEventSize
// 是包级变量（环境变量可覆盖），此处临时调小以驱动该路径；relay 包测试均未启用
// t.Parallel，串行执行下安全。
func TestClassifyPassthroughStreamEndOversizedEvent(t *testing.T) {
	terminal := map[string]struct{}{"response.completed": {}}
	created := `data: {"type":"response.created"}` + "\n\n"
	oversized := `data: {"pad":"` + strings.Repeat("a", 200) + `"}` + "\n\n"

	prev := maxSSEEventSize
	maxSSEEventSize = 64
	defer func() { maxSSEEventSize = prev }()

	// 已解析出事件后遭遇超长事件：未达终态 → truncated。
	if got := classifyPassthroughStreamEnd([]byte(created+oversized), terminal, nil, nil); got != passthroughStreamTruncated {
		t.Fatalf("oversized event after events = %q, want %q", got, passthroughStreamTruncated)
	}
	// 超长事件前已有终态事件：视为完整流 → terminal。
	if got := classifyPassthroughStreamEnd([]byte(created+`data: {"type":"response.completed"}`+"\n\n"+oversized), terminal, nil, nil); got != passthroughStreamTerminal {
		t.Fatalf("oversized event after terminal = %q, want %q", got, passthroughStreamTerminal)
	}
	// 零事件 + 首个事件即超长：无法判断 → unclassified。
	if got := classifyPassthroughStreamEnd([]byte(oversized), terminal, nil, nil); got != passthroughStreamUnclassified {
		t.Fatalf("oversized first event = %q, want %q", got, passthroughStreamUnclassified)
	}
}

// 直通错误事件流的观测性反转（PR-1 的目标缺陷）：payload 已先行透传给客户端、
// 处理器返回 nil（按成功记账），但流尾分类必须把该流标记为 error_event。
func TestPassthroughErrorEventStreamClassifiedErrorEvent(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIResponse, transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	pt := ra.outAdapter.(transformerModel.PassthroughCapable)
	cfg := pt.PassthroughConfig()
	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.failed","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"failed","error":{"code":429,"message":"rate limited"}}}`,
		"",
	}, "\n")
	if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), cfg); err != nil {
		t.Fatalf("error-event passthrough stream is recorded as success today (observational inversion), got error: %v", err)
	}
	if recorder.Body.Len() == 0 {
		t.Fatalf("expected error-event payload forwarded to client verbatim, got empty body")
	}
	kind := classifyPassthroughStreamEnd([]byte(body), cfg.TerminalEvents, cfg.ErrorEvents, cfg.VoidPrefixEvents)
	if kind != passthroughStreamErrorEvent {
		t.Fatalf("expected classifier kind %q for error-event stream, got %q", passthroughStreamErrorEvent, kind)
	}
}
