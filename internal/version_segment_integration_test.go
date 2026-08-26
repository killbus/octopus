package internal

import (
	"context"
	"io"
	"net/url"
	"testing"
	"unicode"

	"github.com/bestruirui/octopus/internal/model"
	relayModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

// TestVersionSegmentIntegration verifies the full chain
// Site.BaseURL → projection → relay outbound URL for the three canonical
// version-segment inputs (bare domain, .../v1, .../v1beta), across the
// OpenAI chat and Gemini route types.
//
// The projection layer and relay canonicalization are unexported in their
// packages; this test reproduces their committed contracts: projection is a
// pure passthrough of model.NormalizeSiteModelEndpointURL (project.go), and
// the relay canonicalizes by trimming the path's trailing slash (baseurl.go).
// The outbound transformers are the authoritative layer under test: they must
// fill the missing version segment (or preserve the existing one) and never
// double-append.
func TestVersionSegmentIntegration(t *testing.T) {
	routeCases := []struct {
		name      string
		routeType model.SiteModelRouteType
		outbound  outbound.OutboundType
	}{
		{name: "openai_chat", routeType: model.SiteModelRouteTypeOpenAIChat, outbound: outbound.OutboundTypeOpenAIChat},
		{name: "gemini", routeType: model.SiteModelRouteTypeGemini, outbound: outbound.OutboundTypeGemini},
	}

	inputCases := []struct {
		name    string
		baseURL string
	}{
		{name: "bare_domain", baseURL: "https://example.com"},
		{name: "v1beta", baseURL: "https://example.com/v1beta"},
		{name: "v1", baseURL: "https://example.com/v1"},
	}

	for _, rc := range routeCases {
		t.Run(rc.name, func(t *testing.T) {
			for _, ic := range inputCases {
				t.Run(ic.name, func(t *testing.T) {
					site := &model.Site{
						Platform:         model.SitePlatformAPI,
						DefaultRouteType: rc.routeType,
						BaseURL:          ic.baseURL,
					}

					// 1. Projection: passthrough, no version segment appended
					//    (mirrors sitesync.buildProjectedChannelBaseURL).
					projected := model.NormalizeSiteModelEndpointURL(site.BaseURL)

					// 2. Channel built from the projection (what the relay reads).
					channel := model.Channel{
						Type:     rc.outbound,
						BaseUrls: []model.BaseUrl{{URL: projected, Delay: 0}},
					}
					relayBaseURL := channel.GetBaseUrl()
					if relayBaseURL != projected {
						t.Fatalf("relay base URL %q != projected base URL %q", relayBaseURL, projected)
					}

					// 3. Effective base URL: what the transformer resolves to
					//    (the single source of truth for version segments).
					effective := model.EffectiveModelBaseURL(relayBaseURL, rc.routeType)

					// 4. Relay outbound: the transformer produces the final URL.
					relayReq, err := outbound.Get(rc.outbound).TransformRequest(
						context.Background(),
						newIntegrationLLMRequest(rc.outbound),
						relayBaseURL,
						"test-key",
					)
					if err != nil {
						t.Fatalf("TransformRequest: %v", err)
					}
					_, _ = io.Copy(io.Discard, relayReq.Body)

					// The effective base URL must carry exactly one version
					// segment (filled for bare domains, preserved for the rest).
					effectivePath := mustParse(t, effective).Path
					if countVersionSegments(effectivePath) != 1 {
						t.Fatalf("effective base URL %q has %d version segments, want 1",
							effective, countVersionSegments(effectivePath))
					}

					// The outbound path must be built on top of the effective
					// base URL path: same leading segments, no second version
					// segment appended by the transformer.
					outPath := relayReq.URL.Path
					if !hasPathPrefix(outPath, effectivePath) {
						t.Fatalf("relay outbound path %q does not start with effective base path %q",
							outPath, effectivePath)
					}
					suffix := outPath[len(effectivePath):]
					if countVersionSegments(suffix) != 0 {
						t.Fatalf("relay outbound path %q contains a duplicated version segment %q after %q",
							outPath, suffix, effectivePath)
					}
				})
			}
		})
	}
}

func newIntegrationLLMRequest(outboundType outbound.OutboundType) *relayModel.InternalLLMRequest {
	switch outboundType {
	case outbound.OutboundTypeGemini:
		return &relayModel.InternalLLMRequest{
			Model: "gemini-2.5-pro",
			Messages: []relayModel.Message{
				{Role: "user", Content: relayModel.MessageContent{Content: stringPtr("hi")}},
			},
		}
	default:
		return &relayModel.InternalLLMRequest{
			Model: "gpt-4o",
			Messages: []relayModel.Message{
				{Role: "user", Content: relayModel.MessageContent{Content: stringPtr("hi")}},
			},
		}
	}
}

func stringPtr(s string) *string { return &s }

func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse URL %q: %v", raw, err)
	}
	return u
}

// countVersionSegments counts path segments matching the shared version-segment
// predicate (v<digit> followed by alphanumerics).
func countVersionSegments(path string) int {
	count := 0
	for _, seg := range pathSegments(path) {
		if isVersionSegmentPath(seg) {
			count++
		}
	}
	return count
}

func pathSegments(path string) []string {
	segs := []string{}
	cur := ""
	for _, c := range path {
		if c == '/' {
			if cur != "" {
				segs = append(segs, cur)
			}
			cur = ""
			continue
		}
		cur += string(c)
	}
	if cur != "" {
		segs = append(segs, cur)
	}
	return segs
}

func isVersionSegmentPath(seg string) bool {
	if len(seg) < 2 || seg[0] != 'v' {
		return false
	}
	if !unicode.Is(unicode.Digit, rune(seg[1])) {
		return false
	}
	for _, c := range seg[2:] {
		if !unicode.Is(unicode.Letter, c) && !unicode.Is(unicode.Digit, c) {
			return false
		}
	}
	return true
}

func hasPathPrefix(path, prefix string) bool {
	return len(path) >= len(prefix) && path[:len(prefix)] == prefix
}
