package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNormalizeSiteModelEndpointURLPreservesRawQuery(t *testing.T) {
	raw := "  https://EXAMPLE.com/v1///?signature=a/+%2F&token=x/&key=1&token=y&api_key=z%2B&key=2  "
	got := NormalizeSiteModelEndpointURL(raw)
	want := "https://EXAMPLE.com/v1?signature=a/+%2F&token=x/&key=1&token=y&api_key=z%2B&key=2"
	if got != want {
		t.Fatalf("NormalizeSiteModelEndpointURL() = %q, want %q", got, want)
	}
}

func TestNormalizeSiteEndpointSetClearsInactiveWeights(t *testing.T) {
	set := NormalizeSiteEndpointSet(SiteEndpointSet{
		BaseURLMode: BaseUrlModeRandom,
		BaseURLs:    []SiteModelEndpoint{{URL: "https://one.example/v1/", Weight: 9}},
	})
	if set.BaseURLs[0].URL != "https://one.example/v1" {
		t.Fatalf("normalized url = %q", set.BaseURLs[0].URL)
	}
	if set.BaseURLs[0].Weight != 0 {
		t.Fatalf("inactive weight = %d, want 0", set.BaseURLs[0].Weight)
	}
}

func TestValidateSiteModelEndpointConfigSumType(t *testing.T) {
	validWeighted := SiteEndpointSet{
		BaseURLMode: BaseUrlModeWeighted,
		BaseURLs: []SiteModelEndpoint{
			{URL: "https://one.example/v1", Weight: 3},
			{URL: "https://two.example/v1?key=a/+%2F", Weight: 1},
		},
	}
	tests := []struct {
		name    string
		config  SiteModelEndpointConfig
		wantErr string
	}{
		{name: "follow", config: FollowSiteModelEndpointConfig()},
		{
			name: "follow rejects endpoint set",
			config: SiteModelEndpointConfig{Default: SiteModelEndpointDefault{
				Source:      SiteModelEndpointSourceFollowSite,
				EndpointSet: &validWeighted,
			}},
			wantErr: "must not include",
		},
		{
			name:    "custom requires endpoint set",
			config:  SiteModelEndpointConfig{Default: SiteModelEndpointDefault{Source: SiteModelEndpointSourceCustom}},
			wantErr: "requires endpoint_set",
		},
		{
			name: "custom weighted",
			config: SiteModelEndpointConfig{Default: SiteModelEndpointDefault{
				Source:      SiteModelEndpointSourceCustom,
				EndpointSet: &validWeighted,
			}},
		},
		{
			name: "duplicate routes",
			config: SiteModelEndpointConfig{
				Default: FollowSiteModelEndpointConfig().Default,
				RouteOverrides: []SiteRouteEndpointSet{
					{RouteType: SiteModelRouteTypeAnthropic, EndpointSet: validWeighted},
					{RouteType: SiteModelRouteTypeAnthropic, EndpointSet: validWeighted},
				},
			},
			wantErr: "duplicate route type",
		},
		{
			name: "duplicate urls",
			config: SiteModelEndpointConfig{Default: SiteModelEndpointDefault{
				Source: SiteModelEndpointSourceCustom,
				EndpointSet: &SiteEndpointSet{
					BaseURLMode: BaseUrlModeDelay,
					BaseURLs: []SiteModelEndpoint{
						{URL: "https://same.example/v1"},
						{URL: "https://same.example/v1"},
					},
				},
			}},
			wantErr: "duplicate model endpoint url",
		},
		{
			name: "fragment rejected",
			config: SiteModelEndpointConfig{Default: SiteModelEndpointDefault{
				Source: SiteModelEndpointSourceCustom,
				EndpointSet: &SiteEndpointSet{
					BaseURLMode: BaseUrlModeDelay,
					BaseURLs:    []SiteModelEndpoint{{URL: "https://one.example/v1#fragment"}},
				},
			}},
			wantErr: "must not include a fragment",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSiteModelEndpointConfig(tt.config)
			if tt.wantErr == "" && err != nil {
				t.Fatalf("ValidateSiteModelEndpointConfig() error = %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestSiteModelEndpointLegacyCompatibility(t *testing.T) {
	legacy := []SiteRouteBaseURL{
		{RouteType: SiteModelRouteTypeAnthropic, BaseURL: "https://example.com/anthropic///?signature=a/+%2F"},
		{RouteType: SiteModelRouteTypeGemini, BaseURL: "https://example.com/gemini"},
	}
	config := NormalizeSiteModelEndpointConfig(SiteModelEndpointConfigFromLegacy(legacy))
	if config.Default.Source != SiteModelEndpointSourceFollowSite {
		t.Fatalf("default source = %q", config.Default.Source)
	}
	if len(config.RouteOverrides) != 2 {
		t.Fatalf("override count = %d", len(config.RouteOverrides))
	}
	got := SiteRouteBaseURLsFromModelEndpointConfig(config)
	if !LegacySiteRouteBaseURLsEquivalent(legacy, []SiteRouteBaseURL{got[1], got[0]}) {
		t.Fatalf("legacy projection is not order-independent: %#v", got)
	}
	if got[0].BaseURL != "https://example.com/anthropic?signature=a/+%2F" {
		t.Fatalf("query-preserving legacy url = %q", got[0].BaseURL)
	}
}

func TestSiteJSONModelEndpointPresence(t *testing.T) {
	var legacy Site
	if err := json.Unmarshal([]byte(`{"name":"legacy","platform":"new-api","base_url":"https://example.com","route_base_urls":[{"route_type":"anthropic","base_url":"https://api.example/v1"}]}`), &legacy); err != nil {
		t.Fatalf("legacy unmarshal failed: %v", err)
	}
	if legacy.ModelEndpointConfig.Default.Source != SiteModelEndpointSourceFollowSite || len(legacy.ModelEndpointConfig.RouteOverrides) != 1 {
		t.Fatalf("legacy config = %#v", legacy.ModelEndpointConfig)
	}

	var nullConfig Site
	if err := json.Unmarshal([]byte(`{"name":"null","platform":"new-api","base_url":"https://example.com","model_endpoint_config":null}`), &nullConfig); err != nil {
		t.Fatalf("null unmarshal failed: %v", err)
	}
	if err := nullConfig.Validate(); err == nil || !strings.Contains(err.Error(), "must not be null") {
		t.Fatalf("null config validation error = %v", err)
	}

	var ambiguous Site
	if err := json.Unmarshal([]byte(`{"name":"both","platform":"new-api","base_url":"https://example.com","model_endpoint_config":{"default":{"source":"follow_site"}},"route_base_urls":[]}`), &ambiguous); err != nil {
		t.Fatalf("ambiguous unmarshal failed: %v", err)
	}
	if err := ambiguous.Validate(); err == nil || !strings.Contains(err.Error(), "must not be provided together") {
		t.Fatalf("ambiguous validation error = %v", err)
	}
}

func TestResolveSiteModelEndpointSetReturnsCloneAndSource(t *testing.T) {
	custom := SiteEndpointSet{
		BaseURLMode: BaseUrlModeFailover,
		BaseURLs:    []SiteModelEndpoint{{URL: "https://default.example/v1"}},
	}
	config := SiteModelEndpointConfig{
		Default: SiteModelEndpointDefault{Source: SiteModelEndpointSourceCustom, EndpointSet: &custom},
		RouteOverrides: []SiteRouteEndpointSet{{
			RouteType: SiteModelRouteTypeAnthropic,
			EndpointSet: SiteEndpointSet{
				BaseURLMode: BaseUrlModeWeighted,
				BaseURLs:    []SiteModelEndpoint{{URL: "https://anthropic.example", Weight: 2}},
			},
		}},
	}
	resolved, source := ResolveSiteModelEndpointSet(config, SiteModelRouteTypeAnthropic, "https://follow.example/v1")
	if source != SiteModelEndpointResolutionRouteOverride || resolved.BaseURLs[0].URL != "https://anthropic.example" {
		t.Fatalf("override resolution = %#v, source=%q", resolved, source)
	}
	resolved.BaseURLs[0].URL = "mutated"
	if config.RouteOverrides[0].EndpointSet.BaseURLs[0].URL == "mutated" {
		t.Fatal("resolved endpoint set aliases config storage")
	}
}
