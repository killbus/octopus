package sitesync

import (
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

func TestBuildModelFetchBaseURLs(t *testing.T) {
	tests := []struct {
		name     string
		site     *model.Site
		expected []string
	}{
		{
			name:     "bare domain returns single candidate",
			site:     &model.Site{Platform: model.SitePlatformAPI, BaseURL: "https://example.com"},
			expected: []string{"https://example.com"},
		},
		{
			name:     "v1beta returns single candidate",
			site:     &model.Site{Platform: model.SitePlatformAPI, BaseURL: "https://example.com/v1beta"},
			expected: []string{"https://example.com/v1beta"},
		},
		{
			name:     "v1 returns single candidate",
			site:     &model.Site{Platform: model.SitePlatformAPI, BaseURL: "https://example.com/v1"},
			expected: []string{"https://example.com/v1"},
		},
		{
			name:     "custom path returns single candidate",
			site:     &model.Site{Platform: model.SitePlatformAPI, BaseURL: "https://example.com/openai"},
			expected: []string{"https://example.com/openai"},
		},
		{
			name:     "nil site returns nil",
			site:     nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := buildModelFetchBaseURLs(tt.site)
			if len(actual) != len(tt.expected) {
				t.Fatalf("expected %d candidates, got %d: %v", len(tt.expected), len(actual), actual)
			}
			for i := range tt.expected {
				if actual[i] != tt.expected[i] {
					t.Fatalf("candidate %d: expected %q, got %q", i, tt.expected[i], actual[i])
				}
			}
		})
	}
}
