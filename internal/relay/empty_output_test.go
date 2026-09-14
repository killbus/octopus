package relay

import (
	"testing"

	tmodel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/samber/lo"
)

// 空输出检测纯函数测试 —— 移植自 fork lingyuins/octopus 2f610094 (PR #155) 的
// empty_output_test.go，仅改 import 路径；本地 Message/Choice 字段形状与 fork 一致
// （design.md「Pure helpers」节，审计已验证）。

// --- isEmptyOutputResponse ---

func TestIsEmptyOutputResponse_NilResponse(t *testing.T) {
	if isEmptyOutputResponse(nil) {
		t.Fatal("nil response should not be empty")
	}
}

func TestIsEmptyOutputResponse_NoChoices(t *testing.T) {
	resp := &tmodel.InternalLLMResponse{Choices: []tmodel.Choice{}}
	if isEmptyOutputResponse(resp) {
		t.Fatal("response with no choices should not be detected as empty output (embedding fallback)")
	}
}

// issue #155 core: model consumed reasoning tokens (CompletionTokens > 0) but no visible content.
func TestIsEmptyOutputResponse_ReasoningTokensOnly(t *testing.T) {
	empty := ""
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{
			Index: 0,
			Message: &tmodel.Message{
				Role:             "assistant",
				Content:          tmodel.MessageContent{Content: &empty},
				ReasoningContent: lo.ToPtr("I thought about this deeply but produced no answer."),
			},
		}},
		Usage: &tmodel.Usage{
			CompletionTokens: 19700,
			CompletionTokensDetails: &tmodel.CompletionTokensDetails{
				ReasoningTokens: 19700,
			},
		},
	}
	if !isEmptyOutputResponse(resp) {
		t.Fatal("reasoning tokens with no visible content should be detected as empty output (issue #155)")
	}
}

func TestIsEmptyOutputResponse_ReasoningFieldOnly(t *testing.T) {
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{
			Index: 0,
			Message: &tmodel.Message{
				Role:      "assistant",
				Reasoning: lo.ToPtr("thinking..."),
			},
		}},
		Usage: &tmodel.Usage{CompletionTokens: 100},
	}
	if !isEmptyOutputResponse(resp) {
		t.Fatal("reasoning field with no visible content should be detected as empty output")
	}
}

func TestIsEmptyOutputResponse_WithTextContent(t *testing.T) {
	content := "Hello!"
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{
			Index: 0,
			Message: &tmodel.Message{
				Role:             "assistant",
				Content:          tmodel.MessageContent{Content: &content},
				ReasoningContent: lo.ToPtr("I thought about what to say."),
			},
		}},
		Usage: &tmodel.Usage{CompletionTokens: 50},
	}
	if isEmptyOutputResponse(resp) {
		t.Fatal("response with visible text content should not be detected as empty")
	}
}

func TestIsEmptyOutputResponse_WithWhitespaceContent(t *testing.T) {
	ws := "   \n  "
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{
			Index: 0,
			Message: &tmodel.Message{
				Role:    "assistant",
				Content: tmodel.MessageContent{Content: &ws},
			},
		}},
		Usage: &tmodel.Usage{CompletionTokens: 10},
	}
	if !isEmptyOutputResponse(resp) {
		t.Fatal("whitespace-only content should be detected as empty output")
	}
}

func TestIsEmptyOutputResponse_WithToolCalls(t *testing.T) {
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{
			Index: 0,
			Message: &tmodel.Message{
				Role:      "assistant",
				ToolCalls: []tmodel.ToolCall{{ID: "call_1", Type: "function"}},
			},
		}},
	}
	if isEmptyOutputResponse(resp) {
		t.Fatal("response with tool calls should not be detected as empty")
	}
}

func TestIsEmptyOutputResponse_WithMultipleContent(t *testing.T) {
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{
			Index: 0,
			Message: &tmodel.Message{
				Role: "assistant",
				Content: tmodel.MessageContent{
					MultipleContent: []tmodel.MessageContentPart{{Type: "text", Text: lo.ToPtr("hi")}},
				},
			},
		}},
	}
	if isEmptyOutputResponse(resp) {
		t.Fatal("response with multimodal content should not be detected as empty")
	}
}

func TestIsEmptyOutputResponse_WithAudio(t *testing.T) {
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{
			Index: 0,
			Message: &tmodel.Message{
				Role: "assistant",
				Audio: &struct {
					Data       string `json:"data,omitempty"`
					ExpiresAt  int64  `json:"expires_at,omitempty"`
					ID         string `json:"id,omitempty"`
					Transcript string `json:"transcript,omitempty"`
				}{Data: "base64audio"},
			},
		}},
	}
	if isEmptyOutputResponse(resp) {
		t.Fatal("response with audio should not be detected as empty")
	}
}

// Team B 审计发现 2（MED）：Refusal 是客户端可见内容（审计裁定链：Refusal is
// client-visible → must count as visible）。非流式 refusal-only 响应（openai
// outbound TransformResponse 设 Message.Refusal）修复前被判空 → flag ON 假重试。
func TestIsEmptyOutputResponse_RefusalOnly(t *testing.T) {
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{
			Index: 0,
			Message: &tmodel.Message{
				Role:    "assistant",
				Refusal: "I cannot help with that request.",
			},
		}},
		Usage: &tmodel.Usage{CompletionTokens: 12},
	}
	if isEmptyOutputResponse(resp) {
		t.Fatal("refusal-only response must count as visible content (not empty output)")
	}
}

func TestStreamChunkHasVisibleContent_RefusalDelta(t *testing.T) {
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{
			Index: 0,
			Delta: &tmodel.Message{
				Role:    "assistant",
				Refusal: "I cannot help with that request.",
			},
		}},
	}
	if !streamChunkHasVisibleContent(resp) {
		t.Fatal("refusal delta chunk must have visible content (chunk path latent gap)")
	}
}

func TestIsEmptyOutputResponse_NilMessage(t *testing.T) {
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{Index: 0, Message: nil}},
		Usage:   &tmodel.Usage{CompletionTokens: 5},
	}
	if !isEmptyOutputResponse(resp) {
		t.Fatal("choice with nil message should be detected as empty output")
	}
}

// --- streamChunkHasVisibleContent ---

func TestStreamChunkHasVisibleContent_NilResponse(t *testing.T) {
	if streamChunkHasVisibleContent(nil) {
		t.Fatal("nil response should not have visible content")
	}
}

func TestStreamChunkHasVisibleContent_ReasoningOnly(t *testing.T) {
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{
			Index: 0,
			Delta: &tmodel.Message{
				Role:             "assistant",
				ReasoningContent: lo.ToPtr("thinking..."),
			},
		}},
	}
	if streamChunkHasVisibleContent(resp) {
		t.Fatal("reasoning-only chunk should not have visible content (issue #155)")
	}
}

func TestStreamChunkHasVisibleContent_TextDelta(t *testing.T) {
	content := "Hello"
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{
			Index: 0,
			Delta: &tmodel.Message{
				Role:    "assistant",
				Content: tmodel.MessageContent{Content: &content},
			},
		}},
	}
	if !streamChunkHasVisibleContent(resp) {
		t.Fatal("text delta chunk should have visible content")
	}
}

func TestStreamChunkHasVisibleContent_ToolCallDelta(t *testing.T) {
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{
			Index: 0,
			Delta: &tmodel.Message{
				Role:      "assistant",
				ToolCalls: []tmodel.ToolCall{{ID: "call_1", Type: "function"}},
			},
		}},
	}
	if !streamChunkHasVisibleContent(resp) {
		t.Fatal("tool call delta chunk should have visible content")
	}
}

func TestStreamChunkHasVisibleContent_EmptyDelta(t *testing.T) {
	resp := &tmodel.InternalLLMResponse{
		Choices: []tmodel.Choice{{
			Index: 0,
			Delta: &tmodel.Message{Role: "assistant"},
		}},
	}
	if streamChunkHasVisibleContent(resp) {
		t.Fatal("empty delta chunk should not have visible content")
	}
}

func TestStreamChunkHasVisibleContent_NoChoices(t *testing.T) {
	resp := &tmodel.InternalLLMResponse{Choices: []tmodel.Choice{}}
	if streamChunkHasVisibleContent(resp) {
		t.Fatal("response with no choices should not have visible content")
	}
}
