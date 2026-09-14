package relay

import (
	"context"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
	openaiOutbound "github.com/bestruirui/octopus/internal/transformer/outbound/openai"
)

// round-5 P2（G2 平行真理消解）表征：streamUsageOptionsInjected 查询注入的
// 持久效果（internalRequest.StreamOptions.IncludeUsage），不再重新推导注入
// 条件。三类契约：
//   - 注入发生（chat 出站 + 流式 + 原地翻转）→ true；
//   - 非流式（无注入）→ false；
//   - 客户端自带 include_usage=true（无注入，但字段在场）→ true——
//     被 400 拒绝时的归因无论字段由谁加上都成立，且未在场时不再误报。
func TestStreamUsageOptionsInjectedQueriesInjectionEffect(t *testing.T) {
	cases := []struct {
		name string
		req  *model.InternalLLMRequest
		want bool
	}{
		{
			name: "nil request",
			req:  nil,
			want: false,
		},
		{
			name: "streaming without stream_options after injection",
			req:  injectedChatRequest(t, nil),
			want: true,
		},
		{
			name: "streaming with include_usage=false flipped by injection",
			req:  injectedChatRequest(t, &model.StreamOptions{IncludeUsage: false}),
			want: true,
		},
		{
			name: "client-supplied include_usage=true",
			req:  injectedChatRequest(t, &model.StreamOptions{IncludeUsage: true}),
			want: true,
		},
		{
			name: "non-streaming never injects",
			req:  &model.InternalLLMRequest{Model: "gpt-4o"},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ra := &relayAttempt{relayRequest: &relayRequest{internalRequest: tc.req}}
			if got := ra.streamUsageOptionsInjected(); got != tc.want {
				t.Fatalf("streamUsageOptionsInjected() = %v, want %v", got, tc.want)
			}
		})
	}
}

// injectedChatRequest 走真实注入点（ChatOutbound.TransformRequest）构造注入后
// 的 internalRequest——表征测试必须驱动同一代码路径，不得手工模拟注入效果。
func injectedChatRequest(t *testing.T, streamOptions *model.StreamOptions) *model.InternalLLMRequest {
	t.Helper()
	stream := true
	req := &model.InternalLLMRequest{
		Model:         "gpt-4o",
		Stream:        &stream,
		StreamOptions: streamOptions,
		Messages: []model.Message{
			{Role: "user", Content: model.MessageContent{Content: stringPtr("hi")}},
		},
	}
	if _, err := (&openaiOutbound.ChatOutbound{}).TransformRequest(context.Background(), req, "https://api.openai.com", "sk-test"); err != nil {
		t.Fatalf("TransformRequest: %v", err)
	}
	return req
}
