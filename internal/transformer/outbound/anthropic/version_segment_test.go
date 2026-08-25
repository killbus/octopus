package anthropic

import (
	"context"
	"io"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

func TestAnthropicMessagesTransformRequest_BareDomain(t *testing.T) {
	outbound := &MessageOutbound{}
	req := &model.InternalLLMRequest{
		Model: "claude-3-5-sonnet-20241022",
		Messages: []model.Message{{
			Role: "user",
			Content: model.MessageContent{
				Content: stringPtr("hello"),
			},
		}},
	}

	httpReq, err := outbound.TransformRequest(context.Background(), req, "https://example.com", "sk-test")
	if err != nil {
		t.Fatalf("TransformRequest: %v", err)
	}
	_, _ = io.Copy(io.Discard, httpReq.Body)

	if httpReq.URL.Path != "/v1/messages" {
		t.Errorf("expected /v1/messages, got %s", httpReq.URL.Path)
	}
}

func TestAnthropicMessagesTransformRequest_WithVersion(t *testing.T) {
	outbound := &MessageOutbound{}
	req := &model.InternalLLMRequest{
		Model: "claude-3-5-sonnet-20241022",
		Messages: []model.Message{{
			Role: "user",
			Content: model.MessageContent{
				Content: stringPtr("hello"),
			},
		}},
	}

	httpReq, err := outbound.TransformRequest(context.Background(), req, "https://example.com/v1", "sk-test")
	if err != nil {
		t.Fatalf("TransformRequest: %v", err)
	}
	_, _ = io.Copy(io.Discard, httpReq.Body)

	if httpReq.URL.Path != "/v1/messages" {
		t.Errorf("expected /v1/messages, got %s", httpReq.URL.Path)
	}
}

func TestAnthropicMessagesTransformRequestRaw_BareDomain(t *testing.T) {
	outbound := &MessageOutbound{}
	rawBody := []byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":16,"messages":[{"role":"user","content":"hello"}]}`)

	httpReq, err := outbound.TransformRequestRaw(
		context.Background(),
		rawBody,
		"claude-3-5-sonnet-20241022",
		"https://example.com",
		"sk-test",
		nil,
	)
	if err != nil {
		t.Fatalf("TransformRequestRaw: %v", err)
	}
	_, _ = io.Copy(io.Discard, httpReq.Body)

	if httpReq.URL.Path != "/v1/messages" {
		t.Errorf("expected /v1/messages, got %s", httpReq.URL.Path)
	}
}
