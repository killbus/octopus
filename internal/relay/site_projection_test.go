package relay

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	sitesvc "github.com/bestruirui/octopus/internal/site"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/gin-gonic/gin"
)

func TestProjectedCustomModelEndpointReachesConfiguredAPIBase(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	var gotPath string
	var gotModel string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read upstream request: %v", err)
		}
		var payload struct {
			Model string `json:"model"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode upstream request: %v", err)
		}
		gotModel = payload.Model
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"chatcmpl_projected","object":"chat.completion","created":1,"model":"deepseek-v4-flash-free","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer upstream.Close()

	channelID := projectCustomEndpointChannel(t, ctx, model.SiteEndpointSet{
		BaseURLs:    []model.SiteModelEndpoint{{URL: upstream.URL + "/zen/v1"}},
		BaseURLMode: model.BaseUrlModeDelay,
	})
	groupName := "opencode/deepseek-v4-flash"
	createProjectedRelayGroup(t, ctx, groupName, channelID)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 71)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"opencode/deepseek-v4-flash","messages":[{"role":"user","content":"hello"}]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIChat, c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected projected endpoint request to succeed, got %d body %s", recorder.Code, recorder.Body.String())
	}
	if gotPath != "/zen/v1/chat/completions" {
		t.Fatalf("upstream path = %q, want %q", gotPath, "/zen/v1/chat/completions")
	}
	if gotModel != "deepseek-v4-flash-free" {
		t.Fatalf("upstream model = %q, want %q", gotModel, "deepseek-v4-flash-free")
	}
}

func TestProjectedCustomModelEndpointFailsOverOnNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	baseURLCooler.resetForTest()
	t.Cleanup(baseURLCooler.resetForTest)

	var firstHits atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHits.Add(1)
		w.Header().Set("Content-Type", "text/html")
		http.Error(w, "<!DOCTYPE html>", http.StatusNotFound)
	}))
	defer first.Close()

	var secondHits atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHits.Add(1)
		if r.URL.Path != "/v1/chat/completions" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"chatcmpl_projected_failover","object":"chat.completion","created":1,"model":"deepseek-v4-flash-free","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer second.Close()

	channelID := projectCustomEndpointChannel(t, ctx, model.SiteEndpointSet{
		BaseURLs: []model.SiteModelEndpoint{
			{URL: first.URL + "/v1"},
			{URL: second.URL + "/v1"},
		},
		BaseURLMode: model.BaseUrlModeFailover,
	})
	groupName := "opencode/deepseek-v4-flash"
	createProjectedRelayGroup(t, ctx, groupName, channelID)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 72)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"opencode/deepseek-v4-flash","messages":[{"role":"user","content":"hello"}]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIChat, c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected projected endpoint 404 failover to succeed, got %d body %s", recorder.Code, recorder.Body.String())
	}
	if firstHits.Load() != 1 || secondHits.Load() != 1 {
		t.Fatalf("expected one request per projected endpoint, got first=%d second=%d", firstHits.Load(), secondHits.Load())
	}
}

func projectCustomEndpointChannel(t *testing.T, ctx context.Context, endpointSet model.SiteEndpointSet) int {
	t.Helper()
	config := model.SiteModelEndpointConfig{Default: model.SiteModelEndpointDefault{
		Source:      model.SiteModelEndpointSourceCustom,
		EndpointSet: &endpointSet,
	}}
	site := &model.Site{
		Name: "OpenCode", Platform: model.SitePlatformAPI,
		BaseURL: "https://control.example", Enabled: true,
		ModelEndpointConfig: config,
	}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	account := &model.SiteAccount{
		SiteID: site.ID, Name: "public", CredentialType: model.SiteCredentialTypeAPIKey,
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
		SiteAccountID: account.ID, GroupKey: "default", ModelName: "deepseek-v4-flash-free",
		RouteType: model.SiteModelRouteTypeOpenAIChat,
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&item).Error; err != nil {
		t.Fatalf("create model: %v", err)
	}

	channelIDs, err := sitesvc.ProjectAccount(ctx, account.ID)
	if err != nil {
		t.Fatalf("ProjectAccount failed: %v", err)
	}
	if len(channelIDs) != 1 {
		t.Fatalf("projected channel ids = %#v, want one channel", channelIDs)
	}
	projected, err := op.ChannelGet(channelIDs[0], ctx)
	if err != nil {
		t.Fatalf("ChannelGet failed: %v", err)
	}
	if projected.BaseUrlMode != endpointSet.BaseURLMode || len(projected.BaseUrls) != len(endpointSet.BaseURLs) {
		t.Fatalf("projected endpoint set = mode %v urls %#v", projected.BaseUrlMode, projected.BaseUrls)
	}
	return projected.ID
}

func createProjectedRelayGroup(t *testing.T, ctx context.Context, groupName string, channelID int) {
	t.Helper()
	group := &model.Group{Name: groupName, Mode: model.GroupModeFailover, RetryEnabled: true, MaxRetries: 1}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{
		GroupID: group.ID, ChannelID: channelID, ModelName: "deepseek-v4-flash-free", Priority: 1, Weight: 1,
	}, ctx); err != nil {
		t.Fatalf("GroupItemAdd failed: %v", err)
	}
}
