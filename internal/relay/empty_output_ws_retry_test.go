package relay

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/relay/stream"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

// Team B 审计发现 1（HIGH）：WS 路径空流重试不可达。
//
// wsUpstreamReader 默认 statusCode=200（transport_ws.go），空流（reasoning-only →
// finalize → ErrEmptyUpstreamStream）不经过任何 close-status/error 事件改写点，
// forwardViaWS 的错误出口原样返回 reader.StatusCode()=200 →
// isRetryableStatus(200)=false → ws_client.go 同通道重试循环立即 break，PR-2 在
// WS 路径上成为 no-op。修复：错误出口对 ErrEmptyUpstreamStream 返回 0（与 HTTP
// dispatcher 的 `return 0, err` 语义对齐；isRetryableStatus(0)=true）。
//
// 两个测试分层：
//  1. TestForwardViaWSEmptyStreamReturnsRetryableStatusCodeZero —— 直接断言
//     forwardViaWS 的返回对 (0, ErrEmptyUpstreamStream)，即循环可行性的前提。
//  2. TestWSRelayRetriesEmptyStreamViaSameChannelLoop —— 驱动 runWSRelay 的
//     真实同通道重试循环（ws_client.go loop），空流后重试触发并最终成功。

// wsReasoningOnlyFrames 构造 WS 上游的 reasoning-only 帧序列（无终态块）。
func wsReasoningOnlyFrames() [][]byte {
	return [][]byte{
		[]byte(`{"type":"response.created","response":{"id":"resp_ws","model":"gpt-4o"}}`),
		[]byte(`{"type":"response.reasoning_summary_text.delta","delta":"thinking hard"}`),
	}
}

// wsGoodFrames 构造 WS 上游的完整帧序列（text delta + 终态）。
func wsGoodFrames() [][]byte {
	return [][]byte{
		[]byte(`{"type":"response.created","response":{"id":"resp_ws2","model":"gpt-4o"}}`),
		[]byte(`{"type":"response.output_text.delta","delta":"ok"}`),
		[]byte(`{"type":"response.completed","response":{"id":"resp_ws2","model":"gpt-4o","status":"completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`),
	}
}

// writeWSFrames 顺序写出全部帧；失败即中断（上游写失败对测试不可恢复）。
func writeWSFrames(t *testing.T, conn *websocket.Conn, frames [][]byte) {
	t.Helper()
	for _, frame := range frames {
		if err := conn.Write(context.Background(), websocket.MessageText, frame); err != nil {
			t.Fatalf("failed to write upstream ws frame: %v", err)
		}
	}
}

// 返回值对级别：flag ON 时 reasoning-only WS 流 → forwardViaWS 返回
// (0, ErrEmptyUpstreamStream)。修复前返回 (200, ErrEmptyUpstreamStream)。
func TestForwardViaWSEmptyStreamReturnsRetryableStatusCodeZero(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetString(model.SettingKeyResponsesWSEnabled, "true"); err != nil {
		t.Fatalf("SettingSetString responses ws enabled failed: %v", err)
	}

	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		if _, _, err := conn.Read(r.Context()); err != nil {
			return
		}
		writeWSFrames(t, conn, wsReasoningOnlyFrames())
		// handler 返回 → normal closure → 客户端读到 io.EOF → finalize → 空流。
	}))
	defer wsServer.Close()

	channel := &model.Channel{
		Name:     "relay-ws-empty-retry",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: wsServer.URL + "/v1"}},
		Model:    "gpt-4o",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "empty-key"}},
		WSMode:   model.ChannelWSModeTransform,
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}

	internalReq := &transformerModel.InternalLLMRequest{
		Model:        "gpt-4o",
		Stream:       boolPtr(true),
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
	}
	req := &relayRequest{
		c:               nil, // WS 入站（非 HTTP gin 请求）
		ctx:             context.Background(),
		inAdapter:       inbound.Get(inbound.InboundTypeOpenAIResponse),
		internalRequest: internalReq,
		metrics:         NewRelayMetrics(1, "gpt-4o", nil, internalReq),
		apiKeyID:        1,
		requestModel:    "gpt-4o",
		groupID:         1,
		groupSessionTTL: 60,
	}
	writer := &notifyStreamWriter{header: http.Header{}}
	req.streamWriter = writer
	ra := &relayAttempt{
		relayRequest:      req,
		outAdapter:        outbound.Get(channel.Type),
		channel:           channel,
		usedKey:           channel.Keys[0],
		emptyRetryEnabled: true,
	}

	statusCode, err := ra.forwardViaWS(context.Background())
	if !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("expected stream.ErrEmptyUpstreamStream for reasoning-only ws stream, got %v", err)
	}
	if statusCode != 0 {
		t.Fatalf("expected statusCode=0 for ws empty stream (retryable same-channel, aligned with HTTP dispatcher), got %d", statusCode)
	}
	if writer.written {
		t.Fatalf("expected nothing written to client (held bytes discarded on abandon), got %q", writer.buf.String())
	}
	wsUpstreamPool.Remove(newWSPoolKey(channel.ID, channel.Keys[0].ID, buildUpstreamWSHeaders(nil, channel, channel.Keys[0].ChannelKey), baseURLKey(channel.GetBaseUrl())))
}

// 循环级别：驱动 runWSRelay 的真实同通道重试循环（ws_client.go loop）。
// 第一次 attempt 走 WS 收到 reasoning-only 空流 → (0, ErrEmptyUpstreamStream) →
// isRetryableStatus(0)=true → 循环继续（修复前 200 → 立即 break，仅 1 次上游命中）。
// 第二次 attempt 因 WS 健康退避（RecordWSFailure 对空流同样计数，既有行为）降级
// HTTP fallback 到同一 channel/key 的同一端点，收到完整流 → 成功。
func TestWSRelayRetriesEmptyStreamViaSameChannelLoop(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetString(model.SettingKeyResponsesWSEnabled, "true"); err != nil {
		t.Fatalf("SettingSetString responses ws enabled failed: %v", err)
	}

	var wsHits, httpHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			wsHits.Add(1)
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close(websocket.StatusNormalClosure, "")
			if _, _, err := conn.Read(r.Context()); err != nil {
				return
			}
			if wsHits.Load() == 1 {
				// 第一次连接：reasoning-only 空流（无终态块）。
				writeWSFrames(t, conn, wsReasoningOnlyFrames())
				return
			}
			// 防御分支：若后续 attempt 再次拨号 WS，给完整流（测试不依赖降级路径）。
			writeWSFrames(t, conn, wsGoodFrames())
			return
		}
		httpHits.Add(1)
		// HTTP fallback：完整 Responses SSE 流。
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flusher", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, line := range []string{
			`data: {"type":"response.created","response":{"id":"resp_http","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"in_progress"}}`,
			"",
			`data: {"type":"response.output_text.delta","delta":"hello"}`,
			"",
			`data: {"type":"response.completed","response":{"id":"resp_http","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
			"",
		} {
			fmt.Fprint(w, line+"\n")
			flusher.Flush()
		}
	}))
	defer server.Close()

	channel := &model.Channel{
		Name:     "relay-ws-empty-retry-loop",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: server.URL + "/v1"}},
		Model:    "gpt-4o",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "loop-key"}},
		WSMode:   model.ChannelWSModeTransform,
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	group := &model.Group{
		Name:              "relay-ws-empty-retry-group",
		Mode:              model.GroupModeFailover,
		RetryEnabled:      true,
		MaxRetries:        2,
		EmptyRetryEnabled: true,
	}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "gpt-4o", Priority: 1, Weight: 1}, ctx); err != nil {
		t.Fatalf("GroupItemAdd failed: %v", err)
	}

	clientConn, serverConn := newTestWSConnPair(t)
	defer clientConn.Close(websocket.StatusNormalClosure, "")
	defer serverConn.Close(websocket.StatusNormalClosure, "")

	internalReq := &transformerModel.InternalLLMRequest{
		Model:        "gpt-4o",
		Stream:       boolPtr(true),
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
	}
	// 迭代器候选用内存副本（GroupItemAdd 只刷新缓存，不回填本地 struct）。
	iterGroup := *group
	iterGroup.Items = []model.GroupItem{{ChannelID: channel.ID, ModelName: "gpt-4o", Priority: 1}}
	iter := balancer.NewIterator(iterGroup, 1, "gpt-4o")

	req := &relayRequest{
		c:               nil,
		ctx:             context.Background(),
		inAdapter:       inbound.Get(inbound.InboundTypeOpenAIResponse),
		internalRequest: internalReq,
		metrics:         NewRelayMetrics(1, "gpt-4o", nil, internalReq),
		apiKeyID:        1,
		requestModel:    "gpt-4o",
		groupID:         group.ID,
		groupSessionTTL: 60,
		iter:            iter,
		streamWriter:    NewWSStreamWriter(context.Background(), serverConn),
	}

	result := runWSRelay(context.Background(), req, group)
	if !result.Success {
		t.Fatalf("expected same-channel retry to succeed after ws empty stream, got err=%v", result.Err)
	}
	if wsHits.Load() != 1 {
		t.Fatalf("expected exactly 1 upstream ws hit (empty stream attempt), got %d", wsHits.Load())
	}
	if total := wsHits.Load() + httpHits.Load(); total != 2 {
		t.Fatalf("expected the retry loop to fire a second attempt (pre-fix the loop breaks on status 200 after 1 hit), got %d upstream hits", total)
	}

	// 下游 WS 客户端必须收到重试后的完整流（含 text delta）。
	var downstream strings.Builder
	for i := 0; i < 10; i++ {
		readCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, data, err := clientConn.Read(readCtx)
		cancel()
		if err != nil {
			break
		}
		downstream.Write(data)
		if strings.Contains(downstream.String(), "hello") {
			break
		}
	}
	if !strings.Contains(downstream.String(), "hello") {
		t.Fatalf("expected downstream client to receive retried stream with text delta, got %q", downstream.String())
	}
	wsUpstreamPool.Remove(newWSPoolKey(channel.ID, channel.Keys[0].ID, buildUpstreamWSHeaders(nil, channel, channel.Keys[0].ChannelKey), baseURLKey(channel.GetBaseUrl())))
}

// 可重试链单元断言：空流返回 0 后链条闭合（isRetryableStatus(0)=true），
// 而修复前的 200 处在不可重试集合内——这正是循环断裂的原因。
func TestIsRetryableStatusForEmptyStreamRetry(t *testing.T) {
	if !isRetryableStatus(0) {
		t.Fatal("status 0 (ws empty stream, aligned with HTTP dispatcher) must be retryable")
	}
	if isRetryableStatus(http.StatusOK) {
		t.Fatal("status 200 must not be retryable (pre-fix ws empty stream carried 200 and broke the loop)")
	}
}

// 设计不变量：空输出分类与同通道重试是正交维度。emptyRetry=ON + retryEnabled=OFF
// （failover 组，effectiveMaxRetries 恒为 1）时，空流判失败并 failover 到下一渠道，
// 客户端收到第二渠道的正文而非第一渠道的空 200。分类在 retryEnabled=OFF 时同样
// 生效且客户端可见，因此该开关常显、不按同通道重试开关条件渲染；唯一保留的耦合
// 是重试预算（retry.go）：空输出的同通道重试次数受 retryEnabled 上界约束。
func TestEmptyRetryClassificationIndependentOfSameChannelRetry(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetString(model.SettingKeyResponsesWSEnabled, "true"); err != nil {
		t.Fatalf("SettingSetString responses ws enabled failed: %v", err)
	}

	var emptyHits, goodHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			// WS 只服务第一渠道；第二渠道用 HTTP（区分命中渠道）。
			emptyHits.Add(1)
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close(websocket.StatusNormalClosure, "")
			if _, _, err := conn.Read(r.Context()); err != nil {
				return
			}
			writeWSFrames(t, conn, wsReasoningOnlyFrames())
			return
		}
		goodHits.Add(1)
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flusher", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, line := range []string{
			`data: {"type":"response.created","response":{"id":"resp_good","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"in_progress"}}`,
			"",
			`data: {"type":"response.output_text.delta","delta":"fallback text"}`,
			"",
			`data: {"type":"response.completed","response":{"id":"resp_good","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
			"",
		} {
			fmt.Fprint(w, line+"\n")
			flusher.Flush()
		}
	}))
	defer server.Close()

	emptyChannel := &model.Channel{
		Name:     "relay-empty-classify-empty",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: server.URL + "/v1"}},
		Model:    "gpt-4o",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "empty-key"}},
		WSMode:   model.ChannelWSModeTransform,
	}
	if err := op.ChannelCreate(emptyChannel, ctx); err != nil {
		t.Fatalf("ChannelCreate empty failed: %v", err)
	}
	goodChannel := &model.Channel{
		Name:     "relay-empty-classify-good",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: server.URL + "/v1"}},
		Model:    "gpt-4o",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "good-key"}},
		WSMode:   model.ChannelWSModeOff, // 兜底渠道纯 HTTP；否则 inherit 会继承系统 WS 默认（passthrough）也拨 WS
	}
	if err := op.ChannelCreate(goodChannel, ctx); err != nil {
		t.Fatalf("ChannelCreate good failed: %v", err)
	}

	// retryEnabled=OFF：分类（判失败→failover）仍须生效；重试预算寄生（裁决③）
	// 只约束同通道循环，failover 切换从不被 retryEnabled gate。
	group := &model.Group{
		Name:              "relay-empty-classify-group",
		Mode:              model.GroupModeFailover,
		RetryEnabled:      false,
		EmptyRetryEnabled: true,
	}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	// failover 模式按 Priority 升序：priority 1 空渠道在前，2 为兜底。
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: emptyChannel.ID, ModelName: "gpt-4o", Priority: 1}, ctx); err != nil {
		t.Fatalf("GroupItemAdd empty failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: goodChannel.ID, ModelName: "gpt-4o", Priority: 2}, ctx); err != nil {
		t.Fatalf("GroupItemAdd good failed: %v", err)
	}

	clientConn, serverConn := newTestWSConnPair(t)
	defer clientConn.Close(websocket.StatusNormalClosure, "")
	defer serverConn.Close(websocket.StatusNormalClosure, "")

	internalReq := &transformerModel.InternalLLMRequest{
		Model:        "gpt-4o",
		Stream:       boolPtr(true),
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
	}
	iterGroup := *group
	iterGroup.Items = []model.GroupItem{
		{ChannelID: emptyChannel.ID, ModelName: "gpt-4o", Priority: 1},
		{ChannelID: goodChannel.ID, ModelName: "gpt-4o", Priority: 2},
	}
	iter := balancer.NewIterator(iterGroup, 1, "gpt-4o")

	req := &relayRequest{
		c:               nil,
		ctx:             context.Background(),
		inAdapter:       inbound.Get(inbound.InboundTypeOpenAIResponse),
		internalRequest: internalReq,
		metrics:         NewRelayMetrics(1, "gpt-4o", nil, internalReq),
		apiKeyID:        1,
		requestModel:    "gpt-4o",
		groupID:         group.ID,
		groupSessionTTL: 60,
		iter:            iter,
		streamWriter:    NewWSStreamWriter(context.Background(), serverConn),
	}

	result := runWSRelay(context.Background(), req, group)
	if !result.Success {
		t.Fatalf("expected failover to second channel after empty stream, got err=%v", result.Err)
	}
	if emptyHits.Load() != 1 {
		t.Fatalf("expected exactly 1 hit on the empty channel (classification, no same-channel retry), got %d", emptyHits.Load())
	}
	if goodHits.Load() != 1 {
		t.Fatalf("expected exactly 1 hit on the fallback channel, got %d", goodHits.Load())
	}

	var downstream strings.Builder
	for i := 0; i < 10; i++ {
		readCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, data, err := clientConn.Read(readCtx)
		cancel()
		if err != nil {
			break
		}
		downstream.Write(data)
		if strings.Contains(downstream.String(), "fallback text") {
			break
		}
	}
	if !strings.Contains(downstream.String(), "fallback text") {
		t.Fatalf("expected client to receive fallback channel's text, not the empty channel's empty 200, got %q", downstream.String())
	}
	wsUpstreamPool.Remove(newWSPoolKey(emptyChannel.ID, emptyChannel.Keys[0].ID, buildUpstreamWSHeaders(nil, emptyChannel, emptyChannel.Keys[0].ChannelKey), baseURLKey(emptyChannel.GetBaseUrl())))
}
