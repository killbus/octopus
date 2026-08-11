package relay

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

func TestHandlerFailsOverBaseURLsWithinChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	baseURLCooler.resetForTest()
	t.Cleanup(baseURLCooler.resetForTest)

	var firstHits atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHits.Add(1)
		http.Error(w, "failed", http.StatusInternalServerError)
	}))
	defer first.Close()

	var secondHits atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"chatcmpl_failover","object":"chat.completion","created":1,"model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer second.Close()

	channel := &model.Channel{
		Name:        "standard-url-failover",
		Type:        outbound.OutboundTypeOpenAIChat,
		Enabled:     true,
		BaseUrlMode: model.BaseUrlModeFailover,
		BaseUrls: []model.BaseUrl{
			{URL: first.URL + "/v1", Delay: 0},
			{URL: second.URL + "/v1", Delay: 1},
		},
		Model: "gpt-4o",
		Keys:  []model.ChannelKey{{Enabled: true, ChannelKey: "chat-key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	group := &model.Group{Name: "standard-url-failover-group", Mode: model.GroupModeFailover, RetryEnabled: true, MaxRetries: 3}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "gpt-4o", Priority: 1, Weight: 1}, ctx); err != nil {
		t.Fatalf("GroupItemAdd failed: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 41)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"standard-url-failover-group","messages":[{"role":"user","content":"hello"}]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIChat, c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected standard failover success, got %d body %s", recorder.Code, recorder.Body.String())
	}
	if firstHits.Load() != 1 || secondHits.Load() != 1 {
		t.Fatalf("expected one request per endpoint, got first=%d second=%d", firstHits.Load(), secondHits.Load())
	}
}

func TestHandlerStopsURLFailoverOn503(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	baseURLCooler.resetForTest()
	t.Cleanup(baseURLCooler.resetForTest)

	var firstHits atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHits.Add(1)
		w.Header().Set("Retry-After", "7")
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer first.Close()
	var secondHits atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer second.Close()

	channel := &model.Channel{
		Name:        "standard-url-stop-503",
		Type:        outbound.OutboundTypeOpenAIChat,
		Enabled:     true,
		BaseUrlMode: model.BaseUrlModeFailover,
		BaseUrls: []model.BaseUrl{
			{URL: first.URL + "/v1", Delay: 0},
			{URL: second.URL + "/v1", Delay: 1},
		},
		Model: "gpt-4o",
		Keys:  []model.ChannelKey{{Enabled: true, ChannelKey: "chat-key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	group := &model.Group{Name: "standard-url-stop-503-group", Mode: model.GroupModeFailover, RetryEnabled: true, MaxRetries: 3}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "gpt-4o", Priority: 1, Weight: 1}, ctx); err != nil {
		t.Fatalf("GroupItemAdd failed: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 44)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"standard-url-stop-503-group","messages":[{"role":"user","content":"hello"}]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIChat, c)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 passthrough, got %d body %s", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("Retry-After") != "7" {
		t.Fatalf("expected Retry-After passthrough, got %q", recorder.Header().Get("Retry-After"))
	}
	if firstHits.Load() != 1 || secondHits.Load() != 0 {
		t.Fatalf("503 must stop URL failover, got first=%d second=%d", firstHits.Load(), secondHits.Load())
	}
}

func TestHandleResponsesCompactFailsOverBaseURLs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	baseURLCooler.resetForTest()
	t.Cleanup(baseURLCooler.resetForTest)

	var firstHits atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHits.Add(1)
		http.Error(w, "failed", http.StatusInternalServerError)
	}))
	defer first.Close()

	var secondHits atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHits.Add(1)
		if r.URL.Path != "/v1/responses/compact" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_cmp_failover","object":"response.compaction","output":[]}`))
	}))
	defer second.Close()

	channel := &model.Channel{
		Name:        "compact-url-failover",
		Type:        outbound.OutboundTypeOpenAIResponse,
		Enabled:     true,
		BaseUrlMode: model.BaseUrlModeFailover,
		BaseUrls: []model.BaseUrl{
			{URL: first.URL + "/v1", Delay: 0},
			{URL: second.URL + "/v1", Delay: 1},
		},
		Model: "compact-model",
		Keys:  []model.ChannelKey{{Enabled: true, ChannelKey: "compact-key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	group := &model.Group{Name: "compact-url-failover-group", Mode: model.GroupModeFailover, RetryEnabled: true, MaxRetries: 3}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "compact-model", Priority: 1, Weight: 1}, ctx); err != nil {
		t.Fatalf("GroupItemAdd failed: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 42)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/compact", strings.NewReader(`{"model":"compact-url-failover-group","input":[{"role":"user","content":"hello"}]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	HandleResponsesCompact(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected compact failover success, got %d body %s", recorder.Code, recorder.Body.String())
	}
	if firstHits.Load() != 1 || secondHits.Load() != 1 {
		t.Fatalf("expected one request per endpoint, got first=%d second=%d", firstHits.Load(), secondHits.Load())
	}
}

func TestImagesHandlerFailsOverBaseURLs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	baseURLCooler.resetForTest()
	t.Cleanup(baseURLCooler.resetForTest)

	var firstHits atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHits.Add(1)
		http.Error(w, "failed", http.StatusInternalServerError)
	}))
	defer first.Close()

	var secondHits atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHits.Add(1)
		if r.URL.Path != "/v1/images/generations" {
			http.Error(w, fmt.Sprintf("unexpected path %s", r.URL.Path), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"created":1,"data":[]}`))
	}))
	defer second.Close()

	channel := &model.Channel{
		Name:        "images-url-failover",
		Type:        outbound.OutboundTypeOpenAIChat,
		Enabled:     true,
		BaseUrlMode: model.BaseUrlModeFailover,
		BaseUrls: []model.BaseUrl{
			{URL: first.URL + "/v1", Delay: 0},
			{URL: second.URL + "/v1", Delay: 1},
		},
		Model: "image-model",
		Keys:  []model.ChannelKey{{Enabled: true, ChannelKey: "image-key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	group := &model.Group{Name: "images-url-failover-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "image-model", Priority: 1, Weight: 1}, ctx); err != nil {
		t.Fatalf("GroupItemAdd failed: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 43)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"images-url-failover-group","prompt":"octopus"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	ImagesHandler("/images/generations", c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected images failover success, got %d body %s", recorder.Code, recorder.Body.String())
	}
	if firstHits.Load() != 1 || secondHits.Load() != 1 {
		t.Fatalf("expected one request per endpoint, got first=%d second=%d", firstHits.Load(), secondHits.Load())
	}
}
