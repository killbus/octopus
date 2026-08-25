package openai

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

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

	if !strings.HasPrefix(httpReq.URL.Path, "/v1/") {
		t.Errorf("expected URL path to start with /v1/, got %s", httpReq.URL.Path)
	}
	if httpReq.URL.Path != "/v1/chat/completions" {
		t.Errorf("expected /v1/chat/completions, got %s", httpReq.URL.Path)
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

	if httpReq.URL.Path != "/v1/responses" {
		t.Errorf("expected /v1/responses, got %s", httpReq.URL.Path)
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

	if httpReq.URL.Path != "/v1/responses" {
		t.Errorf("expected /v1/responses, got %s", httpReq.URL.Path)
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

	if httpReq.URL.Path != "/v1/embeddings" {
		t.Errorf("expected /v1/embeddings, got %s", httpReq.URL.Path)
	}
}
