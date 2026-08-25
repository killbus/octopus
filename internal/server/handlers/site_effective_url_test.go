package handlers

import (
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

func TestSiteEffectiveModelBaseURLField(t *testing.T) {
	site := model.Site{
		BaseURL:        "https://example.com",
		DefaultRouteType: model.SiteModelRouteTypeGemini,
	}
	site.EffectiveModelBaseURL = model.EffectiveModelBaseURL(site.BaseURL, site.DefaultRouteType)

	if site.EffectiveModelBaseURL != "https://example.com/v1beta" {
		t.Errorf("EffectiveModelBaseURL = %q, want %q", site.EffectiveModelBaseURL, "https://example.com/v1beta")
	}

	site2 := model.Site{
		BaseURL:        "https://example.com",
		DefaultRouteType: model.SiteModelRouteTypeOpenAIChat,
	}
	site2.EffectiveModelBaseURL = model.EffectiveModelBaseURL(site2.BaseURL, site2.DefaultRouteType)

	if site2.EffectiveModelBaseURL != "https://example.com/v1" {
		t.Errorf("EffectiveModelBaseURL = %q, want %q", site2.EffectiveModelBaseURL, "https://example.com/v1")
	}

	site3 := model.Site{
		BaseURL:        "https://example.com/v1beta",
		DefaultRouteType: model.SiteModelRouteTypeGemini,
	}
	site3.EffectiveModelBaseURL = model.EffectiveModelBaseURL(site3.BaseURL, site3.DefaultRouteType)

	if site3.EffectiveModelBaseURL != "https://example.com/v1beta" {
		t.Errorf("EffectiveModelBaseURL = %q, want %q", site3.EffectiveModelBaseURL, "https://example.com/v1beta")
	}
}
