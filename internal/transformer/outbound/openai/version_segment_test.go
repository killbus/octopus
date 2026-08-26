package openai

import (
	"context"
	"io"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

// The transformer is a pure operation-path transform: it never fills in a
// version segment. A bare base is used verbatim and only the operation path
// is appended; version-segment completion happens at projection time.
func TestOpenAIChatTransformRequest_BareDomain(t *testing.T) {
	outbound := &ChatOutbound{}
	req := &model.InternalLLMRequest{
		Model: "gpt-4o",
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

	if httpReq.URL.Path != "/chat/completions" {
		t.Errorf("expected bare base + /chat/completions, got %s", httpReq.URL.Path)
	}
}

func TestOpenAIChatTransformRequest_WithVersion(t *testing.T) {
	outbound := &ChatOutbound{}
	req := &model.InternalLLMRequest{
		Model: "gpt-4o",
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

	if httpReq.URL.Path != "/v1/chat/completions" {
		t.Errorf("expected /v1/chat/completions, got %s", httpReq.URL.Path)
	}
}

func TestOpenAIResponseTransformRequest_BareDomain(t *testing.T) {
	outbound := &ResponseOutbound{}
	req := &model.InternalLLMRequest{
		Model: "gpt-4o",
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

	if httpReq.URL.Path != "/responses" {
		t.Errorf("expected bare base + /responses, got %s", httpReq.URL.Path)
	}
}

func TestOpenAIResponseTransformRequestRaw_BareDomain(t *testing.T) {
	outbound := &ResponseOutbound{}
	rawBody := []byte(`{"model":"old-model","input":"hello"}`)

	httpReq, err := outbound.TransformRequestRaw(
		context.Background(),
		rawBody,
		"new-model",
		"https://example.com",
		"sk-test",
		nil,
	)
	if err != nil {
		t.Fatalf("TransformRequestRaw: %v", err)
	}
	_, _ = io.Copy(io.Discard, httpReq.Body)

	if httpReq.URL.Path != "/responses" {
		t.Errorf("expected bare base + /responses, got %s", httpReq.URL.Path)
	}
}

func TestOpenAIEmbeddingTransformRequest_BareDomain(t *testing.T) {
	outbound := &EmbeddingOutbound{}
	req := &model.InternalLLMRequest{
		Model:          "text-embedding-3-small",
		EmbeddingInput: &model.EmbeddingInput{Single: stringPtr("hello")},
	}

	httpReq, err := outbound.TransformRequest(context.Background(), req, "https://example.com", "sk-test")
	if err != nil {
		t.Fatalf("TransformRequest: %v", err)
	}
	_, _ = io.Copy(io.Discard, httpReq.Body)

	if httpReq.URL.Path != "/embeddings" {
		t.Errorf("expected bare base + /embeddings, got %s", httpReq.URL.Path)
	}
}
