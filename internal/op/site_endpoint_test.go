package op

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

func TestSiteModelEndpointConfigRoundTrip(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	weighted := model.SiteEndpointSet{
		BaseURLMode: model.BaseUrlModeWeighted,
		BaseURLs: []model.SiteModelEndpoint{
			{URL: "https://primary.example/v1", Weight: 3},
			{URL: "https://backup.example/v1?signature=a/+%2F", Weight: 1},
		},
	}
	site := &model.Site{
		Name: "endpoint-roundtrip", Platform: model.SitePlatformNewAPI,
		BaseURL: "https://control.example", Enabled: true,
		ModelEndpointConfig: model.SiteModelEndpointConfig{
			Default: model.SiteModelEndpointDefault{Source: model.SiteModelEndpointSourceCustom, EndpointSet: &weighted},
			RouteOverrides: []model.SiteRouteEndpointSet{{
				RouteType: model.SiteModelRouteTypeAnthropic,
				EndpointSet: model.SiteEndpointSet{
					BaseURLMode: model.BaseUrlModeFailover,
					BaseURLs: []model.SiteModelEndpoint{
						{URL: "https://claude-primary.example"},
						{URL: "https://claude-backup.example"},
					},
				},
			}},
		},
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	got, err := SiteGet(site.ID, ctx)
	if err != nil {
		t.Fatalf("SiteGet failed: %v", err)
	}
	if got.ModelEndpointConfig.Default.Source != model.SiteModelEndpointSourceCustom {
		t.Fatalf("default source = %q", got.ModelEndpointConfig.Default.Source)
	}
	if got.ModelEndpointConfig.Default.EndpointSet.BaseURLs[1].URL != "https://backup.example/v1?signature=a/+%2F" {
		t.Fatalf("default endpoints = %#v", got.ModelEndpointConfig.Default.EndpointSet.BaseURLs)
	}
	if len(got.RouteBaseURLs) != 1 || got.RouteBaseURLs[0].BaseURL != "https://claude-primary.example" {
		t.Fatalf("legacy compatibility view = %#v", got.RouteBaseURLs)
	}
}

func TestSiteLegacyEndpointUpdatePreservesMultiURLWhenEquivalent(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site := &model.Site{
		Name: "legacy-noop", Platform: model.SitePlatformNewAPI,
		BaseURL: "https://control.example", Enabled: true,
		ModelEndpointConfig: model.SiteModelEndpointConfig{
			Default: model.FollowSiteModelEndpointConfig().Default,
			RouteOverrides: []model.SiteRouteEndpointSet{{
				RouteType: model.SiteModelRouteTypeAnthropic,
				EndpointSet: model.SiteEndpointSet{
					BaseURLMode: model.BaseUrlModeFailover,
					BaseURLs: []model.SiteModelEndpoint{
						{URL: "https://one.example/v1"},
						{URL: "https://two.example/v1"},
					},
				},
			}},
		},
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	legacyProjection := append([]model.SiteRouteBaseURL(nil), site.RouteBaseURLs...)
	newName := "legacy-noop-renamed"
	updated, err := SiteUpdate(&model.SiteUpdateRequest{ID: site.ID, Name: &newName, RouteBaseURLs: &legacyProjection}, ctx)
	if err != nil {
		t.Fatalf("SiteUpdate failed: %v", err)
	}
	override := updated.ModelEndpointConfig.RouteOverrides[0]
	if override.EndpointSet.BaseURLMode != model.BaseUrlModeFailover || len(override.EndpointSet.BaseURLs) != 2 {
		t.Fatalf("equivalent legacy update collapsed config: %#v", override.EndpointSet)
	}
}

func TestSiteLegacyEndpointUpdateChangesOverridesOnly(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	defaultSet := model.SiteEndpointSet{
		BaseURLMode: model.BaseUrlModeWeighted,
		BaseURLs:    []model.SiteModelEndpoint{{URL: "https://default.example/v1", Weight: 2}},
	}
	site := &model.Site{
		Name: "legacy-change", Platform: model.SitePlatformNewAPI,
		BaseURL: "https://control.example", Enabled: true,
		ModelEndpointConfig: model.SiteModelEndpointConfig{
			Default: model.SiteModelEndpointDefault{Source: model.SiteModelEndpointSourceCustom, EndpointSet: &defaultSet},
		},
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	legacy := []model.SiteRouteBaseURL{{RouteType: model.SiteModelRouteTypeGemini, BaseURL: "https://gemini.example/v1"}}
	updated, err := SiteUpdate(&model.SiteUpdateRequest{ID: site.ID, RouteBaseURLs: &legacy}, ctx)
	if err != nil {
		t.Fatalf("SiteUpdate failed: %v", err)
	}
	if updated.ModelEndpointConfig.Default.Source != model.SiteModelEndpointSourceCustom || updated.ModelEndpointConfig.Default.EndpointSet.BaseURLs[0].URL != "https://default.example/v1" {
		t.Fatalf("legacy update changed default source: %#v", updated.ModelEndpointConfig.Default)
	}
	if len(updated.ModelEndpointConfig.RouteOverrides) != 1 || updated.ModelEndpointConfig.RouteOverrides[0].EndpointSet.BaseURLMode != model.BaseUrlModeDelay {
		t.Fatalf("legacy overrides = %#v", updated.ModelEndpointConfig.RouteOverrides)
	}
}

func TestSiteUpdateRejectsAmbiguousOrNullEndpointConfig(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site := &model.Site{Name: "endpoint-invalid", Platform: model.SitePlatformNewAPI, BaseURL: "https://example.com", Enabled: true}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	config := model.FollowSiteModelEndpointConfig()
	legacy := []model.SiteRouteBaseURL{}
	if _, err := SiteUpdate(&model.SiteUpdateRequest{ID: site.ID, ModelEndpointConfig: &config, RouteBaseURLs: &legacy}, ctx); err == nil {
		t.Fatal("expected ambiguous endpoint update to fail")
	}
	var req model.SiteUpdateRequest
	if err := json.Unmarshal([]byte(`{"id":`+fmt.Sprint(site.ID)+`,"model_endpoint_config":null}`), &req); err != nil {
		t.Fatalf("unmarshal null update: %v", err)
	}
	if _, err := SiteUpdate(&req, ctx); err == nil || !strings.Contains(err.Error(), "must not be null") {
		t.Fatalf("null update error = %v", err)
	}
}
