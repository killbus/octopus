package openai

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

// Characterization: stream_options.include_usage=true is injected on every
// streaming chat-completions request regardless of caller intent. The relay's
// empty-stream discriminator depends on this injection for its usage evidence
// (terminal + no visible output + output_tokens==0 → suspect empty), so the
// injection must survive refactors; stripping it would break the evidence
// chain (see no-strip comment in TransformRequest).
func TestTransformRequestInjectsIncludeUsageOnStreaming(t *testing.T) {
	cases := []struct {
		name          string
		streamOptions *model.StreamOptions
	}{
		{name: "nil_stream_options", streamOptions: nil},
		{name: "include_usage_false", streamOptions: &model.StreamOptions{IncludeUsage: false}},
		{name: "include_usage_true", streamOptions: &model.StreamOptions{IncludeUsage: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outbound := &ChatOutbound{}
			stream := true
			req := &model.InternalLLMRequest{
				Model:         "gpt-4o",
				Stream:        &stream,
				StreamOptions: tc.streamOptions,
				Messages: []model.Message{
					{Role: "user", Content: model.MessageContent{Content: stringPtr("hi")}},
				},
			}
			httpReq, err := outbound.TransformRequest(context.Background(), req, "https://api.openai.com", "sk-test")
			if err != nil {
				t.Fatalf("TransformRequest: %v", err)
			}
			body, err := io.ReadAll(httpReq.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			var payload struct {
				StreamOptions *struct {
					IncludeUsage bool `json:"include_usage"`
				} `json:"stream_options"`
			}
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if payload.StreamOptions == nil {
				t.Fatalf("expected stream_options to be present on streaming request")
			}
			if !payload.StreamOptions.IncludeUsage {
				t.Fatalf("expected stream_options.include_usage=true, got %+v", payload.StreamOptions)
			}
		})
	}
}

// Counter-case: non-streaming requests carry no stream_options at all —
// injection is streaming-only and must not leak into the non-streaming body.
func TestTransformRequestDoesNotInjectIncludeUsageOnNonStreaming(t *testing.T) {
	outbound := &ChatOutbound{}
	req := &model.InternalLLMRequest{
		Model: "gpt-4o",
		Messages: []model.Message{
			{Role: "user", Content: model.MessageContent{Content: stringPtr("hi")}},
		},
	}
	httpReq, err := outbound.TransformRequest(context.Background(), req, "https://api.openai.com", "sk-test")
	if err != nil {
		t.Fatalf("TransformRequest: %v", err)
	}
	body, err := io.ReadAll(httpReq.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := payload["stream_options"]; ok {
		t.Fatalf("expected no stream_options on non-streaming request, got %+v", payload)
	}
}
