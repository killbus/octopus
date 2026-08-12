package sitesync

import (
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
)

func testSiteModelEndpointConfig(overrides ...model.SiteRouteEndpointSet) model.SiteModelEndpointConfig {
	config := model.FollowSiteModelEndpointConfig()
	config.RouteOverrides = overrides
	return config
}

func testSiteRouteEndpoint(routeType model.SiteModelRouteType, rawURL string) model.SiteRouteEndpointSet {
	return model.SiteRouteEndpointSet{
		RouteType: routeType,
		EndpointSet: model.SiteEndpointSet{
			BaseURLs:    []model.SiteModelEndpoint{{URL: rawURL}},
			BaseURLMode: model.BaseUrlModeDelay,
		},
	}
}

func TestMergeProjectedBaseURLsPreservesDelayAndConfigurationOrder(t *testing.T) {
	existing := []model.BaseUrl{
		{URL: "https://old.example/v1", Delay: 31, Weight: 99},
		{URL: "https://keep.example/v1?signature=a/+%2F", Delay: 17, Weight: 99},
	}
	configured := []model.SiteModelEndpoint{
		{URL: "https://keep.example/v1?signature=a/+%2F", Weight: 7},
		{URL: "https://new.example/v1", Weight: 2},
	}
	got := mergeProjectedBaseURLs(existing, configured)
	if len(got) != 2 {
		t.Fatalf("merged endpoints = %#v", got)
	}
	if got[0].URL != configured[0].URL || got[0].Delay != 17 || got[0].Weight != 7 {
		t.Fatalf("preserved endpoint = %#v", got[0])
	}
	if got[1].URL != configured[1].URL || got[1].Delay != 0 || got[1].Weight != 2 {
		t.Fatalf("new endpoint = %#v", got[1])
	}
}

func TestProjectAccountProjectsEndpointSetAndPreservesRuntimeDelay(t *testing.T) {
	ctx := setupProjectTestDB(t)
	defaultSet := model.SiteEndpointSet{
		BaseURLMode: model.BaseUrlModeWeighted,
		BaseURLs: []model.SiteModelEndpoint{
			{URL: "https://primary.example/v1?signature=a/+%2F", Weight: 3},
			{URL: "https://remove.example/v1", Weight: 1},
		},
	}
	site := &model.Site{
		Name: "Endpoint Site", Platform: model.SitePlatformAPI,
		BaseURL: "https://control.example", Enabled: true,
		ModelEndpointConfig: model.SiteModelEndpointConfig{Default: model.SiteModelEndpointDefault{
			Source: model.SiteModelEndpointSourceCustom, EndpointSet: &defaultSet,
		}},
	}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	account := &model.SiteAccount{
		SiteID: site.ID, Name: "API Key Account", CredentialType: model.SiteCredentialTypeAPIKey,
		APIKey: "sk-test", Enabled: true, AutoSync: false, AutoCheckin: false,
	}
	if err := op.SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}
	token := model.SiteToken{
		SiteAccountID: account.ID, Name: "main", Token: "sk-test", GroupKey: "default",
		GroupName: "default", Enabled: true, ValueStatus: model.SiteTokenValueStatusReady,
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&token).Error; err != nil {
		t.Fatalf("create token: %v", err)
	}
	item := model.SiteModel{
		SiteAccountID: account.ID, GroupKey: "default", ModelName: "gpt-4o",
		RouteType: model.SiteModelRouteTypeOpenAIChat,
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&item).Error; err != nil {
		t.Fatalf("create model: %v", err)
	}

	if _, err := ProjectAccount(ctx, account.ID); err != nil {
		t.Fatalf("first ProjectAccount: %v", err)
	}
	channel := loadProjectedChannelsByGroupKey(t, ctx, account.ID)["default"]
	if channel.BaseUrlMode != model.BaseUrlModeWeighted || len(channel.BaseUrls) != 2 {
		t.Fatalf("first projection = mode %v urls %#v", channel.BaseUrlMode, channel.BaseUrls)
	}
	runtimeURLs := append([]model.BaseUrl(nil), channel.BaseUrls...)
	runtimeURLs[0].Delay = 41
	runtimeURLs[0].Weight = 99
	runtimeURLs[1].Delay = 23
	if _, err := op.ChannelUpdate(&model.ChannelUpdateRequest{
		ID: channel.ID, BaseUrls: &runtimeURLs, BypassManagedCheck: true,
	}, ctx); err != nil {
		t.Fatalf("seed runtime delay: %v", err)
	}

	nextSet := model.SiteEndpointSet{
		BaseURLMode: model.BaseUrlModeWeighted,
		BaseURLs: []model.SiteModelEndpoint{
			{URL: "https://primary.example/v1?signature=a/+%2F", Weight: 7},
			{URL: "https://new.example/v1", Weight: 2},
		},
	}
	nextConfig := model.SiteModelEndpointConfig{Default: model.SiteModelEndpointDefault{
		Source: model.SiteModelEndpointSourceCustom, EndpointSet: &nextSet,
	}}
	if _, err := op.SiteUpdate(&model.SiteUpdateRequest{ID: site.ID, ModelEndpointConfig: &nextConfig}, ctx); err != nil {
		t.Fatalf("SiteUpdate endpoint config: %v", err)
	}
	if _, err := ProjectAccount(ctx, account.ID); err != nil {
		t.Fatalf("second ProjectAccount: %v", err)
	}
	updated := loadProjectedChannelsByGroupKey(t, ctx, account.ID)["default"]
	if updated.BaseUrlMode != model.BaseUrlModeWeighted || len(updated.BaseUrls) != 2 {
		t.Fatalf("second projection = mode %v urls %#v", updated.BaseUrlMode, updated.BaseUrls)
	}
	if updated.BaseUrls[0].URL != nextSet.BaseURLs[0].URL || updated.BaseUrls[0].Delay != 41 || updated.BaseUrls[0].Weight != 7 {
		t.Fatalf("preserved projected endpoint = %#v", updated.BaseUrls[0])
	}
	if updated.BaseUrls[1].URL != nextSet.BaseURLs[1].URL || updated.BaseUrls[1].Delay != 0 || updated.BaseUrls[1].Weight != 2 {
		t.Fatalf("new projected endpoint = %#v", updated.BaseUrls[1])
	}
}

func TestProjectAccountClearsOverrideBackToDefaultEndpointSet(t *testing.T) {
	ctx := setupProjectTestDB(t)
	defaultSet := model.SiteEndpointSet{
		BaseURLMode: model.BaseUrlModeWeighted,
		BaseURLs: []model.SiteModelEndpoint{
			{URL: "https://default-primary.example/v1", Weight: 4},
			{URL: "https://default-backup.example/v1", Weight: 1},
		},
	}
	overrideSet := model.SiteEndpointSet{
		BaseURLMode: model.BaseUrlModeFailover,
		BaseURLs: []model.SiteModelEndpoint{
			{URL: "https://override-primary.example/anthropic"},
			{URL: "https://override-backup.example/anthropic"},
		},
	}
	config := model.SiteModelEndpointConfig{
		Default: model.SiteModelEndpointDefault{
			Source:      model.SiteModelEndpointSourceCustom,
			EndpointSet: &defaultSet,
		},
		RouteOverrides: []model.SiteRouteEndpointSet{{
			RouteType:   model.SiteModelRouteTypeAnthropic,
			EndpointSet: overrideSet,
		}},
	}
	site := &model.Site{
		Name: "Override Site", Platform: model.SitePlatformAPI,
		BaseURL: "https://control.example", Enabled: true,
		ModelEndpointConfig: config,
	}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	account := &model.SiteAccount{
		SiteID: site.ID, Name: "Account", CredentialType: model.SiteCredentialTypeAPIKey,
		APIKey: "sk-test", Enabled: true, AutoSync: false, AutoCheckin: false,
	}
	if err := op.SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}
	token := model.SiteToken{
		SiteAccountID: account.ID, Name: "main", Token: "sk-test", GroupKey: "default",
		GroupName: "default", Enabled: true, ValueStatus: model.SiteTokenValueStatusReady,
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&token).Error; err != nil {
		t.Fatalf("create token: %v", err)
	}
	item := model.SiteModel{
		SiteAccountID: account.ID, GroupKey: "default", ModelName: "claude-3-5-sonnet",
		RouteType: model.SiteModelRouteTypeAnthropic,
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&item).Error; err != nil {
		t.Fatalf("create model: %v", err)
	}

	channelIDs, err := ProjectAccount(ctx, account.ID)
	if err != nil {
		t.Fatalf("project override: %v", err)
	}
	projected, err := op.ChannelGet(channelIDs[0], ctx)
	if err != nil {
		t.Fatalf("load override channel: %v", err)
	}
	if projected.BaseUrlMode != model.BaseUrlModeFailover || len(projected.BaseUrls) != 2 || projected.BaseUrls[0].URL != overrideSet.BaseURLs[0].URL {
		t.Fatalf("override projection = mode %v urls %#v", projected.BaseUrlMode, projected.BaseUrls)
	}

	config.RouteOverrides = nil
	if _, err := op.SiteUpdate(&model.SiteUpdateRequest{
		ID: site.ID, ModelEndpointConfig: &config,
	}, ctx); err != nil {
		t.Fatalf("clear override: %v", err)
	}
	channelIDs, err = ProjectAccount(ctx, account.ID)
	if err != nil {
		t.Fatalf("project inherited default: %v", err)
	}
	projected, err = op.ChannelGet(channelIDs[0], ctx)
	if err != nil {
		t.Fatalf("load inherited channel: %v", err)
	}
	if projected.BaseUrlMode != model.BaseUrlModeWeighted || len(projected.BaseUrls) != 2 {
		t.Fatalf("inherited projection = mode %v urls %#v", projected.BaseUrlMode, projected.BaseUrls)
	}
	for index, endpoint := range defaultSet.BaseURLs {
		if projected.BaseUrls[index].URL != endpoint.URL || projected.BaseUrls[index].Weight != endpoint.Weight {
			t.Fatalf("inherited endpoint %d = %#v", index, projected.BaseUrls[index])
		}
	}
}

func TestBuildProjectedChannelBaseURLPreservesQueryBytes(t *testing.T) {
	site := &model.Site{BaseURL: "https://example.com///?signature=a/+%2F&token=x/"}
	want := "https://example.com/v1?signature=a/+%2F&token=x/"
	if got := buildProjectedChannelBaseURL(site); got != want {
		t.Fatalf("buildProjectedChannelBaseURL() = %q, want %q", got, want)
	}
}
