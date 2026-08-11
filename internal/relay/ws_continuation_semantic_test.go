package relay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

func TestHandlerClearsAffinityOnlyForExplicitContinuationSemanticFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	semanticHits := make(chan struct{}, 3)
	semanticServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		semanticHits <- struct{}{}
		defer conn.Close(websocket.StatusNormalClosure, "")
		if _, _, err = conn.Read(r.Context()); err != nil {
			return
		}
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"error","status":409,"error":{"code":"conversation_not_found","type":"invalid_request_error","message":"conversation state is no longer available"}}`))
	}))
	defer semanticServer.Close()

	secondHits := make(chan struct{}, 3)
	secondServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHits <- struct{}{}
		http.Error(w, `{"error":"should not be reached"}`, http.StatusServiceUnavailable)
	}))
	defer secondServer.Close()

	firstChannel := &model.Channel{
		Name:     "relay-ws-semantic-invalid-first",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: semanticServer.URL + "/v1"}},
		Model:    "gpt-4o",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "semantic-first-key"}},
	}
	if err := op.ChannelCreate(firstChannel, ctx); err != nil {
		t.Fatalf("ChannelCreate first channel failed: %v", err)
	}
	secondChannel := &model.Channel{
		Name:     "relay-ws-semantic-invalid-second",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: secondServer.URL + "/v1"}},
		Model:    "gpt-4o",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "semantic-second-key"}},
	}
	if err := op.ChannelCreate(secondChannel, ctx); err != nil {
		t.Fatalf("ChannelCreate second channel failed: %v", err)
	}

	group := &model.Group{Name: "relay-ws-semantic-invalid-group", Mode: model.GroupModeFailover, SessionKeepTime: 60, RetryEnabled: true, MaxRetries: 2}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: firstChannel.ID, ModelName: "gpt-4o", Priority: 1, Weight: 1}, ctx); err != nil {
		t.Fatalf("GroupItemAdd first item failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: secondChannel.ID, ModelName: "gpt-4o", Priority: 2, Weight: 1}, ctx); err != nil {
		t.Fatalf("GroupItemAdd second item failed: %v", err)
	}

	const (
		apiKeyID   = 78
		previousID = "resp_semantic_prev"
	)
	balancer.SetSticky(apiKeyID, group.Name, firstChannel.ID, firstChannel.Keys[0].ID)
	scope := wsAffinityScope{APIKeyID: apiKeyID, GroupID: group.ID, RequestModel: group.Name, ResponseID: previousID}
	if err := getWSAffinityStore().Set(ctx, scope, wsAffinityEntry{
		ChannelID:     firstChannel.ID,
		ChannelKeyID:  firstChannel.Keys[0].ID,
		UpstreamModel: "gpt-4o",
		BaseURLKey:    baseURLKey(firstChannel.GetBaseUrl()),
	}, time.Minute); err != nil {
		t.Fatalf("Set affinity failed: %v", err)
	}
	bindWSResponseConn(previousID, "stale-semantic-conn", time.Minute)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", apiKeyID)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"relay-ws-semantic-invalid-group","previous_response_id":"resp_semantic_prev","input":"hello","stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")

	Handler(inbound.InboundTypeOpenAIResponse, c)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("expected explicit semantic invalidation to return 409, got %d body %s", recorder.Code, recorder.Body.String())
	}
	if len(semanticHits) != 1 {
		t.Fatalf("expected one semantic-invalid upstream attempt, got %d", len(semanticHits))
	}
	if len(secondHits) != 0 {
		t.Fatalf("semantic continuation failure must not fail over to another channel, got %d hits", len(secondHits))
	}
	if sticky := balancer.GetSticky(apiKeyID, group.Name, time.Minute); sticky != nil {
		t.Fatalf("expected channel/key sticky to be cleared after semantic invalidation, got %#v", sticky)
	}
	if affinity, ok := getWSAffinityStore().Get(context.Background(), scope); ok || affinity != nil {
		t.Fatalf("expected response affinity to be cleared after semantic invalidation, got %#v, ok=%t", affinity, ok)
	}
	if connID, ok := getWSResponseConn(previousID); ok || connID != "" {
		t.Fatalf("expected local response-to-connection affinity to be cleared, got %q, ok=%t", connID, ok)
	}
	wsUpstreamPool.Remove(newWSPoolKey(firstChannel.ID, firstChannel.Keys[0].ID, buildUpstreamWSHeaders(c.Request.Header, firstChannel, firstChannel.Keys[0].ChannelKey), baseURLKey(firstChannel.GetBaseUrl())))
}
