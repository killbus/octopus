package model

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestHasVersionSegment(t *testing.T) {
	cases := []struct {
		name string
		path string
		want bool
	}{
		{"v1", "/v1", true},
		{"v1beta", "/v1beta", true},
		{"v1beta2", "/v1beta2", true},
		{"v1alpha", "/v1alpha", true},
		{"v2", "/v2", true},
		{"v999", "/v999", true},
		{"viewer", "/viewer", false},
		{"v1x", "/v1x", true},
		{"empty", "", false},
		{"slash-only", "/", false},
		{"no-version", "/openai", false},
		{"nested-version", "/openai/v1", true},
		{"nested-viewer", "/openai/viewer", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := HasVersionSegment(tc.path); got != tc.want {
				t.Errorf("HasVersionSegment(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

func TestResolveOutboundBaseURL(t *testing.T) {
	cases := []struct {
		name           string
		rawBaseURL     string
		defaultSegment string
		want           string
	}{
		{"bare-domain", "https://example.com", "/v1", "https://example.com/v1"},
		{"bare-domain-slash", "https://example.com/", "/v1", "https://example.com/v1"},
		{"has-v1", "https://example.com/v1", "/v1", "https://example.com/v1"},
		{"has-v1beta", "https://example.com/v1beta", "/v1beta", "https://example.com/v1beta"},
		{"has-v1-default-v1beta", "https://example.com/v1", "/v1beta", "https://example.com/v1"},
		{"trailing-slash", "https://example.com/", "/v1beta", "https://example.com/v1beta"},
		{"duplicate-v1beta-v1", "https://example.com/v1beta/v1", "/v1beta", "https://example.com/v1beta/v1"},
		{"duplicate-v1-v1beta", "https://example.com/v1/v1beta", "/v1", "https://example.com/v1/v1beta"},
		{"duplicate-three", "https://example.com/v1/v1beta/v2", "/v1", "https://example.com/v1/v1beta/v2"},
		{"non-version-path", "https://example.com/openai/v1", "/v1", "https://example.com/openai/v1"},
		{"non-version-first", "https://example.com/v1beta/openai", "/v1beta", "https://example.com/v1beta/openai/v1beta"},
		{"query-preserved", "https://example.com?key=abc", "/v1", "https://example.com/v1?key=abc"},
		{"query-with-path", "https://example.com/foo?key=abc", "/v1", "https://example.com/foo/v1?key=abc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ResolveOutboundBaseURL(tc.rawBaseURL, tc.defaultSegment)
			if got != tc.want {
				t.Errorf("ResolveOutboundBaseURL(%q, %q) = %q, want %q", tc.rawBaseURL, tc.defaultSegment, got, tc.want)
			}
		})
	}
}

func TestEffectiveModelBaseURL_PureFunction(t *testing.T) {
	cases := []struct {
		name      string
		baseURL   string
		routeType SiteModelRouteType
		want      string
	}{
		{"bare-domain-gemini", "https://example.com", SiteModelRouteTypeGemini, "https://example.com/v1beta"},
		{"bare-domain-openai", "https://example.com", SiteModelRouteTypeOpenAIChat, "https://example.com/v1"},
		{"v1beta-gemini", "https://example.com/v1beta", SiteModelRouteTypeGemini, "https://example.com/v1beta"},
		{"v1-openai", "https://example.com/v1", SiteModelRouteTypeOpenAIChat, "https://example.com/v1"},
		{"duplicate-v1beta-v1-gemini", "https://example.com/v1beta/v1", SiteModelRouteTypeGemini, "https://example.com/v1beta/v1"},
		{"bare-domain-anthropic", "https://example.com", SiteModelRouteTypeAnthropic, "https://example.com/v1"},
		{"bare-domain-response", "https://example.com", SiteModelRouteTypeOpenAIResponse, "https://example.com/v1"},
		{"bare-domain-embedding", "https://example.com", SiteModelRouteTypeOpenAIEmbedding, "https://example.com/v1"},
		{"empty-base-url", "", SiteModelRouteTypeGemini, ""},
		{"trailing-slash-gemini", "https://example.com/", SiteModelRouteTypeGemini, "https://example.com/v1beta"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := EffectiveModelBaseURL(tc.baseURL, tc.routeType)
			if got != tc.want {
				t.Errorf("EffectiveModelBaseURL(%q, %q) = %q, want %q", tc.baseURL, tc.routeType, got, tc.want)
			}
		})
	}
}

func TestDefaultVersionSegmentForRouteType(t *testing.T) {
	cases := []struct {
		routeType SiteModelRouteType
		want      string
	}{
		{SiteModelRouteTypeGemini, "/v1beta"},
		{SiteModelRouteTypeOpenAIChat, "/v1"},
		{SiteModelRouteTypeOpenAIResponse, "/v1"},
		{SiteModelRouteTypeOpenAIEmbedding, "/v1"},
		{SiteModelRouteTypeAnthropic, "/v1"},
		{SiteModelRouteTypeVolcengine, "/v1"},
		{SiteModelRouteTypeUnknown, "/v1"},
	}
	for _, tc := range cases {
		t.Run(string(tc.routeType), func(t *testing.T) {
			if got := DefaultVersionSegmentForRouteType(tc.routeType); got != tc.want {
				t.Errorf("DefaultVersionSegmentForRouteType(%q) = %q, want %q", tc.routeType, got, tc.want)
			}
		})
	}
}

func TestVersionSegmentVectors(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "version-segment-vectors.json"))
	if err != nil {
		t.Fatalf("read vector table: %v", err)
	}
	var vectors []struct {
		Name     string          `json:"name"`
		Kind     string          `json:"kind"`
		Input    json.RawMessage `json:"input"`
		Expected json.RawMessage `json:"expected"`
	}
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatalf("parse vector table: %v", err)
	}
	for _, v := range vectors {
		t.Run(v.Name, func(t *testing.T) {
			switch v.Kind {
			case "has_version_segment":
				var in struct{ Path string `json:"path"` }
				if err := json.Unmarshal(v.Input, &in); err != nil {
					t.Fatalf("parse input: %v", err)
				}
				var want bool
				if err := json.Unmarshal(v.Expected, &want); err != nil {
					t.Fatalf("parse expected: %v", err)
				}
				if got := HasVersionSegment(in.Path); got != want {
					t.Errorf("HasVersionSegment(%q) = %v, want %v", in.Path, got, want)
				}
			case "provider_fill":
				var in struct {
					BaseURL  string `json:"base_url"`
					Provider string `json:"provider"`
				}
				if err := json.Unmarshal(v.Input, &in); err != nil {
					t.Fatalf("parse input: %v", err)
				}
				var want string
				if err := json.Unmarshal(v.Expected, &want); err != nil {
					t.Fatalf("parse expected: %v", err)
				}
				seg := "/v1"
				if in.Provider == "gemini" {
					seg = "/v1beta"
				}
				if got := ResolveOutboundBaseURL(in.BaseURL, seg); got != want {
					t.Errorf("ResolveOutboundBaseURL(%q, %q) = %q, want %q", in.BaseURL, seg, got, want)
				}
			}
		})
	}
}
