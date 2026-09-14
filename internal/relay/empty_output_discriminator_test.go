package relay

import (
	"testing"

	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
)

// G4 判别器表测试。案例移植自 research/industry-prior-art-empty-output-retry.md：
// - #155 shell 流（usage 在场 output==0）→ failure
// - #40200 合法空轮（usage 健康）→ healthy
// - reasoning-only（output_tokens == reasoning_tokens > 0）→ healthy
// - new-api ValidUsage 差分（usage 缺失 / 全零 / 部分零）——ValidUsage: prompt!=0 || completion!=0
func TestEvaluateEmptyStreamFailure(t *testing.T) {
	cases := []struct {
		name     string
		visible  bool
		usage    *transformerModel.Usage
		expected emptyFailureVerdict
	}{
		{
			name:    "usage_absent_is_unknown_never_triggers",
			visible: false,
			usage:   nil,
			// new-api ValidUsage(nil)=false 会触发；本地裁定 NULL≠0，契约相对性
			// 待影子期——差分记录在案（differential: new-api triggers, local holds）。
			expected: emptyFailureUnknown,
		},
		{
			name:    "shell_stream_usage_present_output_zero_is_failure",
			visible: false,
			usage:   &transformerModel.Usage{PromptTokens: 194_000},
			// #155：输入计费 194K、输出 0、可见内容零——记账自证。
			expected: emptyFailureFailure,
		},
		{
			name:    "legit_empty_turn_usage_healthy_is_healthy",
			visible: false,
			usage:   &transformerModel.Usage{PromptTokens: 10, CompletionTokens: 1},
			// #40200：上游合法返回空内容轮次，output>0 自证非缺陷。
			expected: emptyFailureHealthy,
		},
		{
			name:    "reasoning_only_output_bills_reasoning_tokens",
			visible: false,
			usage: &transformerModel.Usage{
				PromptTokens:            10,
				CompletionTokens:        500,
				CompletionTokensDetails: &transformerModel.CompletionTokensDetails{ReasoningTokens: 500},
			},
			// reasoning-only 合法流：output_tokens ≥ reasoning_tokens > 0。
			expected: emptyFailureHealthy,
		},
		{
			name:     "all_zero_usage_no_visible_is_failure",
			visible:  false,
			usage:    &transformerModel.Usage{},
			expected: emptyFailureFailure,
		},
		{
			name:     "prompt_nonzero_completion_zero_no_visible_is_failure",
			visible:  false,
			usage:    &transformerModel.Usage{PromptTokens: 5},
			expected: emptyFailureFailure,
		},
		{
			name:    "visible_content_with_zero_usage_defensive_healthy",
			visible: true,
			usage:   &transformerModel.Usage{},
			expected: emptyFailureHealthy,
		},
		{
			name:    "visible_content_with_healthy_usage",
			visible: true,
			usage:   &transformerModel.Usage{PromptTokens: 1, CompletionTokens: 9},
			expected: emptyFailureHealthy,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := evaluateEmptyStreamFailure(tc.visible, tc.usage); got != tc.expected {
				t.Fatalf("expected verdict %d, got %d", tc.expected, got)
			}
		})
	}
}

// 事件路径的 usage 提取：Responses completed 展开的 [MessageStop, UsageDelta] 批次
// 必须同时给出 terminal 与 usage 证据——R2 闸门判别的前提。
func TestObserveStreamEventsExtractsUsageDelta(t *testing.T) {
	usage := &transformerModel.Usage{PromptTokens: 100, CompletionTokens: 0}
	events := []transformerModel.StreamEvent{
		{Kind: transformerModel.StreamEventKindMessageStop},
		{Kind: transformerModel.StreamEventKindUsageDelta, Usage: usage},
	}
	obs := observeStreamEvents(events)
	if !obs.terminal {
		t.Fatal("expected terminal=true from MessageStop")
	}
	if obs.visible {
		t.Fatal("expected visible=false")
	}
	if obs.usage == nil || obs.usage.PromptTokens != 100 {
		t.Fatalf("expected usage extracted from UsageDelta, got %+v", obs.usage)
	}
}

// 响应路径的 usage 提取：chat-completions 最终 usage chunk（choices 为空、顶层 usage）
// 由 observeStreamChunk 捕获。
func TestObserveStreamChunkExtractsTopLevelUsage(t *testing.T) {
	usage := &transformerModel.Usage{PromptTokens: 7, CompletionTokens: 0}
	resp := &transformerModel.InternalLLMResponse{Choices: nil, Usage: usage}
	obs := observeStreamChunk(resp)
	if obs.terminal || obs.visible {
		t.Fatalf("expected no terminal/visible on aux chunk, got %+v", obs)
	}
	if obs.usage == nil {
		t.Fatal("expected top-level usage captured")
	}
}
