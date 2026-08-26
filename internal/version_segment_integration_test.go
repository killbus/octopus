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
// Version-segment completion lives at projection time: the follow_site branch
// of model.ResolveSiteModelEndpointSet fills the bare base with the route
// type's version segment (via model.EffectiveModelBaseURL). The outbound
// transformer is a pure operation-path transform on top of that already-filled
// base — it appends the operation path and never fills or re-fills the version
// segment.
//
// The projection layer's channel build is unexported in its package; this test
// reproduces the committed contracts: the bare follow-site base is
// model.NormalizeSiteModelEndpointURL (buildProjectedChannelBaseURL, project.go),
// and the follow_site resolution fills the version segment
// (ResolveSiteModelEndpointSet, site_endpoint.go).
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

					// 1. Bare follow-site base: a pure normalize passthrough, no
					//    version segment (mirrors sitesync.buildProjectedChannelBaseURL).
					followSiteURL := model.NormalizeSiteModelEndpointURL(site.BaseURL)

					// 2. follow_site projection fills the version segment
					//    (mirrors model.ResolveSiteModelEndpointSet's follow_site branch
					//    → model.EffectiveModelBaseURL). This is the channel base URL
					//    the relay reads, and the authoritative version-segment layer.
					channelBaseURL := model.EffectiveModelBaseURL(followSiteURL, rc.routeType)

					// 3. Channel built from the projection (what the relay reads).
					channel := model.Channel{
						Type:     rc.outbound,
						BaseUrls: []model.BaseUrl{{URL: channelBaseURL, Delay: 0}},
					}
					relayBaseURL := channel.GetBaseUrl()
					if relayBaseURL != channelBaseURL {
						t.Fatalf("relay base URL %q != channel base URL %q", relayBaseURL, channelBaseURL)
					}

					// 4. Relay outbound: the transformer is a pure operation-path
					//    transform — it appends the operation path on top of the
					//    already-filled base and never touches the version segment.
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

					// The channel base URL must carry exactly one version segment
					// (filled for bare domains, preserved for the rest).
					channelPath := mustParse(t, channelBaseURL).Path
					if countVersionSegments(channelPath) != 1 {
						t.Fatalf("channel base URL %q has %d version segments, want 1",
							channelBaseURL, countVersionSegments(channelPath))
					}

					// The outbound path must be built on top of the channel base
					// path: same leading segments, no second version segment
					// appended by the transformer.
					outPath := relayReq.URL.Path
					if !hasPathPrefix(outPath, channelPath) {
						t.Fatalf("relay outbound path %q does not start with channel base path %q",
							outPath, channelPath)
					}
					suffix := outPath[len(channelPath):]
					if countVersionSegments(suffix) != 0 {
						t.Fatalf("relay outbound path %q contains a duplicated version segment %q after %q",
							outPath, suffix, channelPath)
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
