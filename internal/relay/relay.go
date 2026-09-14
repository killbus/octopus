package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/outlierwindow"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/relay/stream"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	openaiOutbound "github.com/bestruirui/octopus/internal/transformer/outbound/openai"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/gin-gonic/gin"
	"github.com/tmaxmax/go-sse"
)

type streamHeartbeatWriter interface {
	Write([]byte) (int, error)
	Flush()
}

func streamHeartbeatInterval() time.Duration {
	interval, err := op.SettingGetInt(dbmodel.SettingKeySSEHeartbeatInterval)
	if err != nil || interval <= 0 {
		return 0
	}
	return time.Duration(interval) * time.Second
}

func newStreamHeartbeatTicker() (*time.Ticker, <-chan time.Time) {
	interval := streamHeartbeatInterval()
	if interval <= 0 {
		return nil, nil
	}
	ticker := time.NewTicker(interval)
	return ticker, ticker.C
}

func writeSSEHeartbeat(writer streamHeartbeatWriter) error {
	if _, err := writer.Write([]byte(":\n\n")); err != nil {
		return err
	}
	writer.Flush()
	return nil
}

func Handler(inboundType inbound.InboundType, c *gin.Context) {
	// 解析请求
	rawBody, internalRequest, inAdapter, err := parseRequest(inboundType, c)
	if err != nil {
		return
	}
	supportedModels := c.GetString("supported_models")
	if supportedModels != "" {
		supportedModelsArray := strings.Split(supportedModels, ",")
		if !slices.Contains(supportedModelsArray, internalRequest.Model) {
			resp.ErrorWithCode(c, http.StatusBadRequest, CodeRelayModelNotSupported, "model not supported")
			return
		}
	}

	requestModel := internalRequest.Model
	apiKeyID := c.GetInt("api_key_id")

	// 获取通道分组
	group, err := op.GroupGetEnabledMap(requestModel, c.Request.Context())
	if err != nil {
		resp.ErrorWithCode(c, http.StatusNotFound, CodeRelayModelNotFound, "model not found")
		return
	}

	// === HTTP Replay 机制 ===
	// 当 HTTP 请求携带 previous_response_id 时，尝试从本地加载上一次成功的 replay 状态，
	// 优先路由到同一渠道/key，并将请求转为自包含形式（合并历史，移除 previous_response_id）。
	var responsesReplayState *wsConversationState
	var requestedPreviousResponseID string
	if inboundType == inbound.InboundTypeOpenAIResponse && internalRequest.RawAPIFormat == model.APIFormatOpenAIResponse {
		requestedPreviousResponseID = internalRequest.OpenAIPreviousResponseID()
		if prevID := requestedPreviousResponseID; prevID != "" {
			responsesReplayState = resolveResponsesReplayState(apiKeyID, group.ID, requestModel, internalRequest)
			if responsesReplayState != nil {
				log.Debugf("loaded HTTP replay state (apikey=%d, group=%d, model=%s, previous_response_id=%s, channel=%d, key=%d)",
					apiKeyID, group.ID, requestModel, prevID, responsesReplayState.ChannelID, responsesReplayState.ChannelKeyID)
				// 转换请求为自包含形式（移除 previous_response_id，合并历史）
				// BuildReplayRequest 返回 nil 表示合并失败，应保留原始请求
				if replayed := responsesReplayState.BuildReplayRequest(internalRequest); replayed != nil {
					internalRequest = replayed
					log.Debugf("HTTP replay request transformed (apikey=%d, removed previous_response_id, merged history)", apiKeyID)
				} else {
					log.Warnf("HTTP replay history merge failed (apikey=%d, group=%d, model=%s, previous_response_id=%s), keeping original request",
						apiKeyID, group.ID, requestModel, prevID)
					responsesReplayState = nil // 放弃 replay，使用原始请求
				}
			} else {
				log.Debugf("no HTTP replay state found (apikey=%d, group=%d, model=%s, previous_response_id=%s)",
					apiKeyID, group.ID, requestModel, prevID)
			}
		}
	}

	// 创建迭代器（策略排序 + 粘性优先）
	// 如果有 replay state，注入为 sticky 偏好
	var preferredSticky *balancer.SessionEntry
	if responsesReplayState != nil {
		preferredSticky = responsesReplayStateToSticky(responsesReplayState)
		if preferredSticky != nil {
			log.Debugf("HTTP replay sticky routing preference (channel=%d, key=%d)", preferredSticky.ChannelID, preferredSticky.ChannelKeyID)
		}
	}
	if preferredSticky == nil && requestedPreviousResponseID != "" && requiresUpstreamWSContinuation(internalRequest) {
		scope := wsAffinityScope{
			APIKeyID:     apiKeyID,
			GroupID:      group.ID,
			RequestModel: requestModel,
			ResponseID:   requestedPreviousResponseID,
		}
		if entry, ok := getWSAffinityStore().Get(c.Request.Context(), scope); ok {
			preferredSticky = &balancer.SessionEntry{
				ChannelID:    entry.ChannelID,
				ChannelKeyID: entry.ChannelKeyID,
				Timestamp:    time.Now(),
			}
			log.Debugf("HTTP continuation affinity hit (channel=%d, key=%d)", entry.ChannelID, entry.ChannelKeyID)
		}
	}
	iter := balancer.NewIteratorWithPreference(group, apiKeyID, requestModel, preferredSticky)
	if iter.Len() == 0 {
		resp.ErrorWithCode(c, http.StatusServiceUnavailable, CodeRelayNoAvailableChannel, "no available channel")
		return
	}

	// === 早期心跳 ===
	// 在所有 forward / 重试 / 退避之前启动早期心跳协程，覆盖前置阶段（连接慢、failover、退避叠加）
	// 期间向客户端发 SSE 注释字节，避免被 Cloudflare 在 120s 零字节阈值上判 524。
	// 仅对流式请求生效；非流式无法发送 SSE 注释（破坏 application/json 协议），
	// 不施加任何本地超时——上游慢响应应让其自然完成或由上游/CF 自身处理。
	isStream := internalRequest.Stream != nil && *internalRequest.Stream
	hb := startEarlyHeartbeat(c, isStream)
	defer hb.Stop()

	// 初始化 Metrics
	metrics := NewRelayMetrics(apiKeyID, requestModel, rawBody, internalRequest)
	// 如果触发了 HTTP replay，记录 ws_mode=replay 和 ws_recovery=replay
	if responsesReplayState != nil {
		metrics.SetWSMode(dbmodel.RelayLogWSModeReplay)
		metrics.SetWSRecovery(dbmodel.RelayLogWSRecoveryReplay)
	}
	responsesPassthroughRequired := internalRequest.HasOpenAIResponsesPassthrough()
	responsesPassthroughCapableFound := false

	// 请求级上下文
	req := &relayRequest{
		c:               c,
		inAdapter:       inAdapter,
		internalRequest: internalRequest,
		metrics:         metrics,
		apiKeyID:        apiKeyID,
		requestModel:    requestModel,
		groupID:         group.ID,
		groupSessionTTL: group.SessionKeepTime,
		iter:            iter,
		rawBody:         rawBody,
		heartbeat:       hb,
	}

	var lastErr error
	var lastResult attemptResult

	for iter.Next() {
		select {
		case <-c.Request.Context().Done():
			log.Debugf("request context canceled, stopping retry")
			metrics.SaveWithChannelStats(c.Request.Context(), false, context.Canceled, iter.Attempts(), false)
			return
		default:
		}

		item := iter.Item()

		// 获取通道
		channel, err := op.ChannelGet(item.ChannelID, c.Request.Context())
		if err != nil {
			log.Warnf("failed to get channel %d: %v", item.ChannelID, err)
			iter.Skip(item.ChannelID, 0, fmt.Sprintf("channel_%d", item.ChannelID), fmt.Sprintf("channel not found: %v", err))
			lastErr = err
			continue
		}
		if !channel.Enabled {
			iter.Skip(channel.ID, 0, channel.Name, "channel disabled")
			continue
		}
		if responsesPassthroughRequired {
			if channel.Type == outbound.OutboundTypeOpenAIResponse {
				responsesPassthroughCapableFound = true
			} else {
				iter.Skip(channel.ID, 0, channel.Name, "openai responses passthrough required")
				continue
			}
		}

		// 出站适配器
		outAdapter := outbound.Get(channel.Type)
		if outAdapter == nil {
			iter.Skip(channel.ID, 0, channel.Name, fmt.Sprintf("unsupported channel type: %d", channel.Type))
			continue
		}

		// 类型兼容性检查
		if internalRequest.IsEmbeddingRequest() && !outbound.IsEmbeddingChannelType(channel.Type) {
			iter.Skip(channel.ID, 0, channel.Name, "channel type not compatible with embedding request")
			continue
		}
		if internalRequest.IsChatRequest() && !outbound.IsChatChannelType(channel.Type) {
			iter.Skip(channel.ID, 0, channel.Name, "channel type not compatible with chat request")
			continue
		}

		// 设置实际模型
		internalRequest.Model = item.ModelName

		log.Debugf("request model %s, mode: %d, forwarding to channel: %s model: %s (attempt %d/%d, sticky=%t)",
			requestModel, group.Mode, channel.Name, item.ModelName,
			iter.Index()+1, iter.Len(), iter.IsSticky())

		selectOpts := dbmodel.ChannelKeySelectOptions{
			ExcludeKeyIDs:  make(map[int]struct{}),
			PreferredKeyID: iter.StickyKeyID(),
		}
		var usedKey dbmodel.ChannelKey
		for {
			usedKey = channel.GetChannelKey(selectOpts)
			if usedKey.ChannelKey == "" {
				break
			}
			if !iter.SkipCircuitBreak(channel.ID, usedKey.ID, channel.Name) {
				break
			}
			selectOpts.ExcludeKeyIDs[usedKey.ID] = struct{}{}
			usedKey = dbmodel.ChannelKey{}
		}
		if usedKey.ChannelKey == "" {
			if len(selectOpts.ExcludeKeyIDs) == 0 {
				iter.Skip(channel.ID, 0, channel.Name, "no available key")
			}
			continue
		}

		// 同通道重试次数：failover 模式下强制 1（URL 切换本身即重试语义），
		// 且必须 per-iteration 重算——maxSameChannelRetries 是循环外固定变量，
		// 整体改写会被首个 failover 渠道污染并波及后续非 failover 渠道。
		effectiveMaxRetries := effectiveSameChannelRetries(group.RetryEnabled, group.MaxRetries, channel.BaseUrlMode)

		// 同通道重试循环
		var result attemptResult
		for retryNum := 0; retryNum < effectiveMaxRetries; retryNum++ {
			// 尝试开始观测（空输出重试端口项）：与退避日志区分，标记每次 attempt 的起点，
			// 便于从 Attempts[] 之外的日志直接还原重试序列。
			log.Debugf("attempt start %d/%d for channel %s (model=%s)",
				retryNum+1, effectiveMaxRetries, channel.Name, item.ModelName)
			// 重试前等待退避
			if retryNum > 0 {
				delay := computeBackoff(retryNum, result.RetryAfter)
				log.Infof("same-channel retry %d/%d for %s, waiting %v",
					retryNum, effectiveMaxRetries, channel.Name, delay)
				select {
				case <-c.Request.Context().Done():
					log.Debugf("request context canceled during retry backoff")
					metrics.SaveWithChannelStats(c.Request.Context(), false, context.Canceled, iter.Attempts(), false)
					return
				case <-time.After(delay):
				}

				// 重建 outAdapter 以重置流式状态（toolIndex, toolCalls 等）
				outAdapter = outbound.Get(channel.Type)
			}

			// 构造尝试级上下文
			ra := &relayAttempt{
				relayRequest:         req,
				outAdapter:           outAdapter,
				channel:              channel,
				usedKey:              usedKey,
				firstTokenTimeOutSec: group.FirstTokenTimeOut,
				emptyRetryEnabled:    group.EmptyRetryEnabled,
			}

			result = ra.attempt()
			if result.Success || result.Written || result.Canceled || result.ResetConversation || result.FirstTokenTimeout || !isRetryableStatus(result.StatusCode) {
				break
			}
		}

		// 同通道重试耗尽后记录熔断器失败
		if !result.Success && !result.Written && !result.Canceled && !result.ResetConversation &&
			!(result.StopFailover && isTransientUpstreamTransportError(result.Err)) {
			failureKind := circuitFailureKind(group.RetryEnabled, result.StatusCode)
			balancer.RecordFailure(channel.ID, usedKey.ID, internalRequest.Model, failureKind)
			outlierwindow.Report(channel.ID, false, result.StatusCode, time.Now())
			if failureKind == balancer.FailureHard {
				maybeLearnManagedRoute(c.Request.Context(), channel.ID, internalRequest.Model, inboundType, result.Err)
			}
		}

		if result.Success {
			outlierwindow.Report(channel.ID, true, result.StatusCode, time.Now())

			// === HTTP Replay 状态保存 ===
			// 成功后，如果是 OpenAI Responses HTTP 请求，保存 replay 状态供后续续接
			// 注意：exact replay 请求成功后也需要保存新状态，否则只能续接一轮
			// 优先使用 metrics.InternalResponse（streaming 安全），避免二次 GetInternalResponse 消耗聚合器
			if inboundType == inbound.InboundTypeOpenAIResponse &&
				req.internalRequest.RawAPIFormat == model.APIFormatOpenAIResponse {
				internalResponse := metrics.InternalResponse
				if internalResponse == nil {
					var err error
					internalResponse, err = inAdapter.GetInternalResponse(c.Request.Context())
					if err != nil {
						log.Debugf("failed to get internal response for replay state save: %v", err)
					}
				}
				if internalResponse != nil {
					// 如果是 exact replay 请求，基于已有状态继续累积
					var newState *wsConversationState
					if req.internalRequest.IsOpenAIExactReplayRequest() && responsesReplayState != nil {
						newState = cloneWSConversationState(responsesReplayState)
						if newState != nil {
							newState.ChannelID = channel.ID
							newState.ChannelKeyID = usedKey.ID
						}
					}
					if newState == nil {
						newState = &wsConversationState{
							RequestModel: requestModel,
							ChannelID:    channel.ID,
							ChannelKeyID: usedKey.ID,
						}
					}
					newState.ApplySuccessfulTurn(req.internalRequest, internalResponse)
					if newState.LastResponseID != "" {
						ttl := wsConversationStateTTL(group.SessionKeepTime)
						storeResponsesReplayState(apiKeyID, group.ID, requestModel, newState, ttl)
						log.Debugf("saved HTTP replay state (apikey=%d, group=%d, model=%s, response_id=%s, channel=%d, key=%d, ttl=%v, is_replay=%t)",
							apiKeyID, group.ID, requestModel, newState.LastResponseID, channel.ID, usedKey.ID, ttl, req.internalRequest.IsOpenAIExactReplayRequest())
					}
				}
			}

			metrics.SaveWithChannelStats(c.Request.Context(), true, nil, iter.Attempts(), false)
			return
		}
		if result.Canceled {
			metrics.SaveWithChannelStats(c.Request.Context(), false, result.Err, iter.Attempts(), false)
			return
		}
		if result.ResetConversation {
			balancer.DeleteSticky(req.apiKeyID, req.requestModel)
			clearContinuationAffinity(c.Request.Context(), req)
			metrics.SaveWithChannelStats(c.Request.Context(), false, result.Err, iter.Attempts(), false)
			if publicErr, ok := classifyWSPublicError(result.Err, result.StatusCode); ok {
				hb.FlushOrError(c, publicErr.Status, publicErr.Message)
			} else {
				hb.FlushOrError(c, result.StatusCode, result.Err.Error())
			}
			return
		}
		if result.StopFailover {
			metrics.SaveWithChannelStats(c.Request.Context(), false, result.Err, iter.Attempts(), false)
			statusCode := result.StatusCode
			if statusCode < http.StatusBadRequest {
				statusCode = http.StatusBadGateway
			}
			hb.FlushOrError(c, statusCode, "channel failed")
			return
		}
		if result.Written {
			metrics.SaveWithChannelStats(c.Request.Context(), false, result.Err, iter.Attempts(), false)
			return
		}
		lastErr = result.Err
		lastResult = result
	}

	// 所有候选通道均失败
	if responsesPassthroughRequired && !responsesPassthroughCapableFound {
		err := fmt.Errorf("openai responses native tools require an openai responses channel")
		metrics.SaveWithChannelStats(c.Request.Context(), false, err, iter.Attempts(), false)
		hb.FlushOrError(c, http.StatusBadRequest, "当前请求包含 OpenAI Responses 原生工具，仅支持 OpenAI Responses 通道直通")
		return
	}
	metrics.SaveWithChannelStats(c.Request.Context(), false, lastErr, iter.Attempts(), false)

	// 透传 429/503 状态码和 Retry-After 头，让客户端 SDK 的重试机制接管
	if isPassthroughStatus(lastResult.StatusCode) {
		if lastResult.RetryAfter > 0 {
			c.Header("Retry-After", fmt.Sprintf("%d", int(lastResult.RetryAfter.Seconds())))
		}
		hb.FlushOrError(c, lastResult.StatusCode, "channel failed")
		return
	}
	if lastResult.StatusCode > 0 {
		hb.FlushOrError(c, lastResult.StatusCode, "channel failed")
		return
	}
	hb.FlushOrError(c, http.StatusBadGateway, "channel failed")
}

func circuitFailureKind(retryEnabled bool, statusCode int) balancer.FailureKind {
	if retryEnabled && isPassthroughStatus(statusCode) {
		return balancer.FailureSoftRateLimit
	}
	return balancer.FailureHard
}

// attempt 统一管理一次通道尝试的完整生命周期
func (ra *relayAttempt) attempt() attemptResult {
	span := ra.iter.StartAttempt(ra.channel.ID, ra.usedKey.ID, ra.channel.Name)

	// URL 层：解析候选端点。
	// - continuation 请求在 resolveBaseUrl 层直接应用 affinity 端点（R4，禁 failover）。
	// - 非 failover 模式取第一个候选（delay=Delay 最小，random/weighted=按权重随机排序后首项）。
	// - failover 模式逐个尝试候选，失败切下一条（连接错误/5xx/404），429/503/其他 4xx/已写/continuation 立即停。
	statusCode, fwdErr := ra.forwardWithBaseURLFailover(span)
	span.SetBaseURLKey(ra.baseURLKey)

	// 更新 channel key 状态
	ra.usedKey.StatusCode = statusCode
	ra.usedKey.LastUseTimeStamp = time.Now().Unix()

	if fwdErr == nil {
		// ====== 成功 ======
		if ra.baseURL != "" {
			baseURLCooler.recordSuccess(ra.channel.ID, canonicalBaseURL(ra.baseURL))
		}
		// Passthrough handlers collect response at stream end via PassthroughConfig.CollectMetrics
		ra.collectResponse()
		ra.usedKey.TotalCost += ra.metrics.Stats.InputCost + ra.metrics.Stats.OutputCost
		op.ChannelKeyUpdate(ra.usedKey)

		span.End(dbmodel.AttemptSuccess, statusCode, "")

		// Channel 维度统计
		op.StatsChannelUpdate(ra.channel.ID, dbmodel.StatsMetrics{
			WaitTime:       span.Duration().Milliseconds(),
			RequestSuccess: 1,
		})

		// 熔断器：记录成功
		balancer.RecordSuccess(ra.channel.ID, ra.usedKey.ID, ra.internalRequest.Model)
		// 会话保持：更新粘性记录
		balancer.SetSticky(ra.apiKeyID, ra.requestModel, ra.channel.ID, ra.usedKey.ID)

		return attemptResult{Success: true}
	}

	// ====== 失败 ======
	if isClientCancellation(ra.requestContext(), fwdErr) {
		written := ra.streamPayloadWritten.Load()
		if written {
			ra.collectResponse()
		}
		op.ChannelKeyUpdate(ra.usedKey)
		span.End(dbmodel.AttemptFailed, statusCode, fwdErr.Error())
		return attemptResult{
			Success:    false,
			Written:    written,
			Canceled:   true,
			Err:        fwdErr,
			StatusCode: statusCode,
		}
	}

	op.ChannelKeyUpdate(ra.usedKey)
	span.End(dbmodel.AttemptFailed, statusCode, fwdErr.Error())

	// 影子观测（G2）：chat-completions 流式出站由 ChatOutbound.TransformRequest
	// 无条件注入 stream_options.include_usage=true（将 usage 合同升级为必填，空输出
	// 判别器依赖该字段）。上游以 400 拒绝注入时在此归因——纯日志，零行为变更，
	// 400 的重试/透传语义不变。
	if statusCode == http.StatusBadRequest && ra.streamUsageOptionsInjected() {
		log.Warnw("relay.include_usage_rejected",
			"start_time_unix", ra.metrics.StartTime.Unix(),
			"api_key_id", ra.apiKeyID,
			"group_id", ra.groupID,
			"channel_id", ra.channel.ID,
			"channel", ra.channelNameForLog(),
			"model", ra.requestModel,
		)
	}

	// Channel 维度统计
	op.StatsChannelUpdate(ra.channel.ID, dbmodel.StatsMetrics{
		WaitTime:      span.Duration().Milliseconds(),
		RequestFailed: 1,
	})

	// 注意：熔断器记录已移至 Handler() 的同通道重试循环外，
	// 避免重试期间过早触发熔断

	written := ra.streamPayloadWritten.Load()
	if written {
		ra.collectResponse()
	}
	firstTokenTimeout := isFirstTokenTimeout(nil, fwdErr)
	continuation := requiresUpstreamWSContinuation(ra.internalRequest)
	// A transport-classified continuation failure is retryable regardless of an
	// inconsistent status carried by the upstream event. Keep the routing tuple
	// pinned, retry it as a gateway failure, and return 502 only after exhaustion.
	if continuation && isTransientUpstreamTransportError(fwdErr) {
		statusCode = http.StatusBadGateway
	}
	resetConversation := continuation && statusCode == http.StatusConflict && needsConversationRestart(relayErrorMessage(fwdErr))
	return attemptResult{
		Success:           false,
		Written:           written,
		ResetConversation: resetConversation,
		StopFailover:      continuation,
		FirstTokenTimeout: firstTokenTimeout,
		Err:               fmt.Errorf("channel %s failed: %w", ra.channel.Name, fwdErr),
		StatusCode:        statusCode,
		RetryAfter:        ra.retryAfter,
	}
}

// forwardWithBaseURLFailover 在 attempt 内执行 URL 层选择与失败切换。
// 返回最终 statusCode 与 error。只在 failover 模式下切换端点；其余模式单次转发。
func (ra *relayAttempt) forwardWithBaseURLFailover(span *balancer.AttemptSpan) (int, error) {
	// continuation 禁 failover（R4）：在 resolveBaseUrl 层直接应用 affinity 端点并单次转发，
	// 不进入 URL 失败切换循环——传输型状态物理上无法跨端点续会话。
	if requiresUpstreamWSContinuation(ra.internalRequest) {
		ra.applyContinuationAffinity(ra.requestContext())
		return ra.forward()
	}

	mode := ra.channel.BaseUrlMode.Normalize()
	if mode != dbmodel.BaseUrlModeFailover {
		cand := resolveSingleBaseURL(ra.channel)
		if cand.URL != "" {
			ra.baseURL = cand.URL
			ra.baseURLKey = cand.Key
		}
		return ra.forward()
	}

	candidates := resolveBaseURLs(ra.channel)
	if len(candidates) == 0 {
		return ra.forward()
	}
	var statusCode int
	var fwdErr error
	for idx, cand := range candidates {
		ra.baseURL = cand.URL
		ra.baseURLKey = cand.Key
		statusCode, fwdErr = ra.forward()
		if fwdErr == nil {
			return statusCode, nil
		}
		// 冷却表记录：仅端点不可用信号（连接错误/5xx/首 token 超时）才记录失败，
		// 429/503/其他 4xx/客户端取消/continuation 不记。最后一条候选同样记录，
		// 使"全部 URL 冷却中时 fail-open 全试"（R7）可经正常失败路径触达。
		if cand.CanonicalURL != "" && ra.isURLCoolableFailure(statusCode, fwdErr) {
			baseURLCooler.recordFailure(ra.channel.ID, cand.CanonicalURL)
		}
		if ra.stopURLFailover(idx, len(candidates), statusCode, fwdErr) {
			return statusCode, fwdErr
		}
	}
	return statusCode, fwdErr
}

// isURLCoolableFailure 判定一次失败是否为"端点不可用"信号、应记入 per-(channel,URL) 冷却表。
// 冷却信号 = 连接错误(status=0)/5xx/首 token 超时；404 仅切换到下一 URL，不冷却整个端点；
// 其他 4xx（共享请求或凭据错误）、已写首字节（端点实际工作过）、客户端取消、continuation（禁 failover）不记。
func (ra *relayAttempt) isURLCoolableFailure(statusCode int, fwdErr error) bool {
	return isBaseURLCoolableFailureSignal(
		ra.requestContext(),
		statusCode,
		fwdErr,
		ra.streamPayloadWritten.Load(),
		requiresUpstreamWSContinuation(ra.internalRequest),
	)
}

// stopURLFailover 判定是否应停止 URL 层失败切换：
// 最后一个候选、已写首字节、continuation、取消、429/503、除 404 外的 4xx 都立即停止。
func (ra *relayAttempt) stopURLFailover(idx, total int, statusCode int, fwdErr error) bool {
	if idx >= total-1 {
		return true
	}
	return !isBaseURLFailoverSignal(
		ra.requestContext(),
		statusCode,
		fwdErr,
		ra.streamPayloadWritten.Load(),
		requiresUpstreamWSContinuation(ra.internalRequest),
	)
}

// parseRequest 解析并验证入站请求
// 返回值中的 rawBody 为客户端原始请求字节，供同格式直通路径重用。
func parseRequest(inboundType inbound.InboundType, c *gin.Context) ([]byte, *model.InternalLLMRequest, model.Inbound, error) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return nil, nil, nil, err
	}

	inAdapter := inbound.Get(inboundType)
	internalRequest, err := inAdapter.TransformRequest(c.Request.Context(), body)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return nil, nil, nil, err
	}

	// Pass through the original query parameters
	internalRequest.Query = c.Request.URL.Query()

	if err := internalRequest.Validate(); err != nil {
		resp.Error(c, http.StatusBadRequest, err.Error())
		return nil, nil, nil, err
	}

	return body, internalRequest, inAdapter, nil
}

// forward 转发请求到上游服务
func (ra *relayAttempt) forward() (int, error) {
	ctx := ra.requestContext()

	// 尝试上游 WebSocket（仅 OpenAI Response outbound 类型；必须是客户端 WS 入站且新开关显式启用）
	if ra.channel.Type == outbound.OutboundTypeOpenAIResponse &&
		ra.internalRequest.RawAPIFormat == model.APIFormatOpenAIResponse {

		shouldTryWS := false
		// Passthrough is now handled by forwardViaHTTP via PassthroughCapable interface
		if ra.internalRequest.IsOpenAIExactReplayRequest() {
			shouldTryWS = false
		} else if ra.c == nil {
			wsMode := effectiveResponsesWSMode(ra.channel)
			shouldTryWS = shouldEnableResponsesWS(ra.channel) && wsMode != responsesWSModeOff
		} else if requiresUpstreamWSContinuation(ra.internalRequest) {
			// Safety: HTTP ingress must not proactively use upstream WS for fresh requests,
			// but an explicit continuation cannot be safely failovered as ordinary HTTP.
			shouldTryWS = true
		}

		if shouldTryWS {
			statusCode, err := ra.forwardViaWS(ctx)
			if statusCode != -1 {
				return statusCode, err
			}
			if requiresUpstreamWSContinuation(ra.internalRequest) {
				return http.StatusBadGateway, fmt.Errorf("upstream continuation transport temporarily unavailable")
			}
			ra.metrics.SetWSRecovery(dbmodel.RelayLogWSRecoveryDowngrade)
			// statusCode == -1 means WS not available, fall through to HTTP
		}
	}

	if requiresUpstreamWSContinuation(ra.internalRequest) {
		// HTTP fallback continuation：沿用 affinity URL 语义，避免 WS/HTTP 两路分裂（R3）
		ra.applyContinuationAffinity(ctx)
	}
	return ra.forwardViaHTTP(ctx)
}

// forwardViaWS attempts to forward via upstream WebSocket.
// Returns statusCode=-1 if WS is not available (caller should fall through to HTTP).
func (ra *relayAttempt) forwardViaWS(ctx context.Context) (int, error) {
	if ra.c == nil && effectiveResponsesWSMode(ra.channel) == responsesWSModePassthrough && !ra.internalRequest.IsOpenAIExactReplayRequest() {
		return ra.forwardViaWSPassthrough(ctx)
	}
	continuation := requiresUpstreamWSContinuation(ra.internalRequest)
	preferredConnID := ""
	if continuation {
		preferredConnID, _ = getWSResponseConn(currentPreviousResponseID(ra.internalRequest))
		// 续会话必须回原端点（R3）：从 affinity 反查 URL 并覆盖 baseURL，
		// 保证 WS 池命中与上次相同 poolKey 桶。
		ra.applyContinuationAffinity(ctx)
	}
	pc := TryUpstreamWSWithPreference(ctx, ra.channel, ra.effectiveBaseURL(), ra.usedKey.ChannelKey, ra.usedKey.ID, ra.clientRequestHeaders(), preferredConnID)
	if pc == nil {
		log.Debugf("upstream WS unavailable for channel %s (key=%d, continuation=%t)", ra.channel.Name, ra.usedKey.ID, continuation)
		return -1, nil // WS not available
	}

	log.Debugf("using upstream WebSocket for channel %s (key=%d)", ra.channel.Name, ra.usedKey.ID)
	log.Debugf("upstream WS selected (channel=%s, key=%d, continuation=%t, previous_response_id=%s)",
		ra.channel.Name, ra.usedKey.ID, continuation, currentPreviousResponseID(ra.internalRequest))

	// Build the Responses API request body
	responsesReq := openaiOutbound.ConvertToResponsesRequest(ra.internalRequest)
	reqBody, err := json.Marshal(responsesReq)
	if err != nil {
		wsUpstreamPool.Put(pc)
		return -1, nil // fall through to HTTP
	}
	ra.metrics.SetTransportRequestPayload(reqBody, ra.internalRequest.Model)

	// Send response.create message
	if err := wsUpstreamPool.SendResponseCreate(ctx, pc, reqBody); err != nil {
		log.Warnf("upstream WS send failed for channel %s: %v", ra.channel.Name, err)
		log.Debugf("upstream WS send failed before stream start (channel=%s, key=%d, continuation=%t, err=%v)",
			ra.channel.Name, ra.usedKey.ID, continuation, err)
		wsUpstreamPool.RemoveConn(pc)
		if isUpstreamWSConnectionBroken(err) {
			log.Debugf("upstream WS send failure eligible for redial (channel=%s, key=%d, continuation=%t)",
				ra.channel.Name, ra.usedKey.ID, continuation)
			statusCode, redialErr, recovered := ra.retryViaFreshUpstreamWS(ctx, reqBody)
			if recovered || redialErr != nil {
				return statusCode, redialErr
			}
			if requiresUpstreamWSContinuation(ra.internalRequest) {
				return http.StatusBadGateway, fmt.Errorf("upstream continuation transport temporarily unavailable")
			}
		}
		wsUpstreamPool.RecordWSFailure(ra.channel.ID, baseURLKey(ra.effectiveBaseURL()))
		return -1, nil // fall through to HTTP
	}

	// Read events from WS and process through the transform pipeline
	ra.metrics.UsedWS = true
	ra.metrics.SetWSExecMode(dbmodel.RelayLogWSExecModeTransform)
	if ra.metrics.WSMode == nil {
		ra.metrics.SetWSMode(defaultWSModeForRequest(ra.internalRequest))
	}
	reader := newWSUpstreamReader(pc, ra.channel.ID, ra.usedKey.ID)
	err = ra.handleWSStreamResponseV2(ctx, reader)
	if err != nil {
		reader.CloseWithError()
		log.Debugf("upstream WS stream failed (channel=%s, key=%d, continuation=%t, written=%t, status=%d, err=%v)",
			ra.channel.Name, ra.usedKey.ID, continuation, ra.getStreamWriter().Written(), reader.StatusCode(), err)
		if requiresUpstreamWSContinuation(ra.internalRequest) && !ra.streamPayloadWritten.Load() && shouldReconnectUpstreamWSBeforeReplay(err) {
			log.Debugf("upstream WS stream failure eligible for reconnect before replay (channel=%s, key=%d, previous_response_id=%s)",
				ra.channel.Name, ra.usedKey.ID, currentPreviousResponseID(ra.internalRequest))
			statusCode, redialErr, recovered := ra.retryViaFreshUpstreamWS(ctx, reqBody)
			if recovered || redialErr != nil {
				return statusCode, redialErr
			}
		}
		if requiresUpstreamWSContinuation(ra.internalRequest) && needsConversationRestart(relayErrorMessage(err)) {
			return http.StatusConflict, err
		}
		if ra.requestContext().Err() == nil {
			wsUpstreamPool.RecordWSFailure(ra.channel.ID, baseURLKey(ra.effectiveBaseURL()))
		}
		// 空流（reasoning-only → ErrEmptyUpstreamStream）是同通道可重试信号，与 HTTP
		// dispatcher 的 `return 0, err` 语义对齐（isRetryableStatus(0)=true）。reader
		// 默认 statusCode=200（transport_ws.go），空流不经过任何改写点，原样返回会使
		// ws_client 重试循环因 isRetryableStatus(200)=false 立即 break。其余错误保留
		// reader.StatusCode()——上游 error 事件的状态码透传是有意设计（Team B 审计发现 1）。
		if errors.Is(err, stream.ErrEmptyUpstreamStream) {
			return 0, err
		}
		return reader.StatusCode(), err
	}

	reader.Close()
	wsUpstreamPool.RecordWSSuccess(ra.channel.ID, baseURLKey(ra.effectiveBaseURL()))
	ra.recordSuccessfulWSAffinity(pc)
	return 200, nil
}

func (ra *relayAttempt) retryViaFreshUpstreamWS(ctx context.Context, reqBody []byte) (int, error, bool) {
	log.Debugf("attempting fresh upstream WS redial (channel=%s, key=%d, previous_response_id=%s)",
		ra.channel.Name, ra.usedKey.ID, currentPreviousResponseID(ra.internalRequest))
	redialed := TryUpstreamWS(ctx, ra.channel, ra.effectiveBaseURL(), ra.usedKey.ChannelKey, ra.usedKey.ID, ra.clientRequestHeaders(), true)
	if redialed == nil {
		log.Debugf("fresh upstream WS redial unavailable (channel=%s, key=%d)", ra.channel.Name, ra.usedKey.ID)
		return 0, nil, false
	}

	retryErr := wsUpstreamPool.SendResponseCreate(ctx, redialed, reqBody)
	if retryErr != nil {
		log.Warnf("upstream WS redial send failed for channel %s: %v", ra.channel.Name, retryErr)
		log.Debugf("fresh upstream WS redial send failed (channel=%s, key=%d, err=%v)", ra.channel.Name, ra.usedKey.ID, retryErr)
		wsUpstreamPool.RemoveConn(redialed)
		wsUpstreamPool.RecordWSFailure(ra.channel.ID, baseURLKey(ra.effectiveBaseURL()))
		if requiresUpstreamWSContinuation(ra.internalRequest) {
			return http.StatusBadGateway, fmt.Errorf("upstream continuation transport temporarily unavailable: %w", retryErr), true
		}
		return -1, nil, true
	}

	ra.metrics.UsedWS = true
	ra.metrics.SetWSExecMode(dbmodel.RelayLogWSExecModeTransform)
	if ra.metrics.WSMode == nil {
		ra.metrics.SetWSMode(defaultWSModeForRequest(ra.internalRequest))
	}
	ra.metrics.SetWSRecovery(dbmodel.RelayLogWSRecoveryReconnect)
	reader := newWSUpstreamReader(redialed, ra.channel.ID, ra.usedKey.ID)
	streamErr := ra.handleWSStreamResponseV2(ctx, reader)
	if streamErr != nil {
		reader.CloseWithError()
		log.Debugf("fresh upstream WS redial stream failed (channel=%s, key=%d, status=%d, err=%v)",
			ra.channel.Name, ra.usedKey.ID, reader.StatusCode(), streamErr)
		if requiresUpstreamWSContinuation(ra.internalRequest) && needsConversationRestart(relayErrorMessage(streamErr)) {
			return http.StatusConflict, streamErr, true
		}
		if ra.requestContext().Err() == nil {
			wsUpstreamPool.RecordWSFailure(ra.channel.ID, baseURLKey(ra.effectiveBaseURL()))
		}
		// 同 forwardViaWS：空流返回 0（可重试语义），其余保留 reader.StatusCode()
		// 的透传语义（Team B 审计发现 1）。
		if errors.Is(streamErr, stream.ErrEmptyUpstreamStream) {
			return 0, streamErr, true
		}
		return reader.StatusCode(), streamErr, true
	}
	log.Debugf("fresh upstream WS redial succeeded (channel=%s, key=%d, previous_response_id=%s)",
		ra.channel.Name, ra.usedKey.ID, currentPreviousResponseID(ra.internalRequest))
	reader.Close()
	wsUpstreamPool.RecordWSSuccess(ra.channel.ID, baseURLKey(ra.effectiveBaseURL()))
	ra.recordSuccessfulWSAffinity(redialed)
	return http.StatusOK, nil, true
}

func (ra *relayAttempt) clientRequestHeaders() http.Header {
	if ra == nil || ra.c == nil || ra.c.Request == nil {
		return nil
	}
	return ra.c.Request.Header
}

func (ra *relayAttempt) handleWSStreamResponseV2(ctx context.Context, reader *wsUpstreamReader) error {
	defer ra.closeFirstTokenBudget()

	// Hand off early heartbeat
	ra.heartbeat.Hand()

	// Build transform function
	hold := newEmptyOutputHold(ra.emptyRetryEnabled)
	transform := hold.wrapTransform(ra)

	// Determine first token timeout
	var firstTokenTimeout time.Duration
	if ra.firstTokenTimeOutSec > 0 && ra.firstTokenBudget == nil {
		firstTokenTimeout = time.Duration(ra.firstTokenTimeOutSec) * time.Second
	}

	// Create StreamProcessor
	processor := stream.NewStreamProcessor(stream.StreamConfig{
		Source:            stream.NewWSSource(reader),
		Transform:         transform,
		Writer:            ra.getStreamWriter(),
		Context:           ctx,
		FirstTokenTimeout: firstTokenTimeout,
		HeartbeatInterval: streamHeartbeatInterval(),
		// 同 handleStreamResponseV2：OFF 时不接线，legacy 定时器语义不变。
		OnHeldChunk: holdOnHeldChunk(ra),
		OnFirstToken: func() {
			ra.metrics.SetFirstTokenTime(time.Now())
			ra.stopFirstTokenTimer()
		},
	})

	// Run processor
	err := processor.Run()

	// Track payload written for metrics collection
	if processor.PayloadWritten() {
		ra.streamPayloadWritten.Store(true)
	}

	// 流结束原因：进 RelayMetrics，relay.complete / empty_stream 行携带（G3）。
	ra.recordStreamEndReason(processor.EndReason())

	// 断连残余观测（同 handleStreamResponseV2）。
	if hold.holding() && hold.heldBytes() > 0 {
		log.Debugf("empty-output hold discarded %d buffered bytes on ws stream end (written=%t)",
			hold.heldBytes(), processor.PayloadWritten())
	}

	// round-5 前置①（路径错位闭合）：WS transform 路径的流尾 usage 形态影子行
	// （observe-only，与 HTTP transform 路径同函数）。
	ra.emitTransformEmptyStreamUsageShadow(hold, processor.EndReason())

	// Handle first token timeout specifically
	if err != nil && strings.Contains(err.Error(), "first token timeout") {
		return ra.firstTokenTimeoutError()
	}

	// Check for context cancellation with first token timeout
	if err != nil {
		if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, err); timeoutErr != nil {
			return timeoutErr
		}
	}

	return err
}

// forwardViaHTTP forwards the request using traditional HTTP.
func (ra *relayAttempt) forwardViaHTTP(ctx context.Context) (int, error) {
	// Check for passthrough capability using interface
	if pt, ok := ra.outAdapter.(model.PassthroughCapable); ok &&
		len(ra.rawBody) > 0 &&
		pt.CanPassthrough(ra.internalRequest.RawAPIFormat) {
		// Additional checks for OpenAI Responses edge cases
		if ra.internalRequest.RawAPIFormat == model.APIFormatOpenAIResponse {
			if ra.c == nil || ra.internalRequest.IsOpenAIExactReplayRequest() || requiresUpstreamWSContinuation(ra.internalRequest) {
				// Fall through to standard path
			} else {
				return ra.forwardViaHTTPPassthrough(ctx, pt)
			}
		} else {
			return ra.forwardViaHTTPPassthrough(ctx, pt)
		}
	}

	return ra.forwardViaHTTPStandard(ctx)
}

// forwardViaHTTPPassthrough handles unified passthrough for any PassthroughCapable transformer.
func (ra *relayAttempt) forwardViaHTTPPassthrough(ctx context.Context, pt model.PassthroughCapable) (int, error) {
	// Build request via TransformRequestRaw
	outboundRequest, err := pt.TransformRequestRaw(
		ctx,
		ra.rawBody,
		ra.internalRequest.Model,
		ra.effectiveBaseURL(),
		ra.usedKey.ChannelKey,
		ra.internalRequest.Query,
	)
	if err != nil {
		log.Warnf("failed to create passthrough request: %v", err)
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	// Apply param overrides
	if err := ra.applyParamOverride(outboundRequest); err != nil {
		return 0, err
	}

	// Copy headers
	ra.copyHeaders(outboundRequest)
	if ra.channel.Type == outbound.OutboundTypeOpenAIResponse {
		outboundRequest.Header.Set("Content-Type", "application/json")
	}

	// Send request
	response, err := ra.sendRequest(outboundRequest)
	if err != nil {
		return 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer response.Body.Close()

	// Check status
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		ra.retryAfter = parseRetryAfter(response.Header.Get("Retry-After"))
		body, _ := io.ReadAll(response.Body)
		statusCode := normalizeUpstreamStatusCode(response.StatusCode, string(body))
		log.Warnf("upstream error from channel %s: status=%d, body=%s", ra.channel.Name, response.StatusCode, string(body))
		return statusCode, fmt.Errorf("upstream error: %d: %s", response.StatusCode, string(body))
	}

	// Get passthrough config
	cfg := pt.PassthroughConfig()

	// Branch: streaming vs non-streaming
	if ra.internalRequest.Stream != nil && *ra.internalRequest.Stream {
		if err := ra.handleStreamResponsePassthroughV2(ctx, response, cfg); err != nil {
			return 0, err
		}
		return response.StatusCode, nil
	}
	return response.StatusCode, ra.handleResponsePassthrough(ctx, response, cfg)
}

// handleResponsePassthrough handles non-streaming passthrough responses.
func (ra *relayAttempt) handleResponsePassthrough(ctx context.Context, response *http.Response, cfg model.PassthroughConfig) error {
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	contentType := response.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}
	ra.c.Data(http.StatusOK, contentType, body)

	// Sidecar metrics parse
	sidecarResp := &http.Response{
		StatusCode: response.StatusCode,
		Header:     response.Header.Clone(),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
	if internalResponse, err := ra.outAdapter.TransformResponse(ctx, sidecarResp); err == nil && internalResponse != nil {
		ra.inAdapter.TransformResponse(ctx, internalResponse)
		if cfg.CollectMetrics {
			ra.collectResponse()
		}
	}

	return nil
}

// forwardViaHTTPStandard 是 forwardViaHTTP 的原路径（直通判定失败时的兜底）。
// 留作显式出口，避免 passthrough 失败时的递归。
func (ra *relayAttempt) forwardViaHTTPStandard(ctx context.Context) (int, error) {
	outboundRequest, err := ra.outAdapter.TransformRequest(
		ctx,
		ra.internalRequest,
		ra.effectiveBaseURL(),
		ra.usedKey.ChannelKey,
	)
	if err != nil {
		log.Warnf("failed to create request: %v", err)
		return 0, fmt.Errorf("failed to create request: %w", err)
	}
	if err := ra.applyParamOverride(outboundRequest); err != nil {
		return 0, err
	}

	// 复制请求头
	ra.copyHeaders(outboundRequest)
	if ra.channel.Type == outbound.OutboundTypeOpenAIResponse {
		outboundRequest.Header.Set("Content-Type", "application/json")
	}

	// 发送请求
	response, err := ra.sendRequest(outboundRequest)
	if err != nil {
		return 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer response.Body.Close()

	// 检查响应状态
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		ra.retryAfter = parseRetryAfter(response.Header.Get("Retry-After"))
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return response.StatusCode, fmt.Errorf("failed to read response body: %w", err)
		}
		statusCode := normalizeUpstreamStatusCode(response.StatusCode, string(body))
		log.Warnf("upstream error from channel %s: status=%d, body=%s", ra.channel.Name, response.StatusCode, string(body))
		return statusCode, fmt.Errorf("upstream error: %d: %s", response.StatusCode, string(body))
	}

	// 处理响应
	if ra.internalRequest.Stream != nil && *ra.internalRequest.Stream {
		// Use V2 StreamProcessor-based implementation
		if err := ra.handleStreamResponseV2(ctx, response); err != nil {
			return 0, err
		}
		return response.StatusCode, nil
	}
	if err := ra.handleResponse(ctx, response); err != nil {
		return 0, err
	}
	return response.StatusCode, nil
}

func defaultWSModeForRequest(req *model.InternalLLMRequest) dbmodel.RelayLogWSMode {
	if requiresUpstreamWSContinuation(req) {
		return dbmodel.RelayLogWSModeContinuation
	}
	return dbmodel.RelayLogWSModeFresh
}

func readOutboundRequestBody(req *http.Request) ([]byte, error) {
	if req == nil || req.Body == nil {
		return nil, nil
	}
	if req.GetBody != nil {
		bodyReader, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		defer bodyReader.Close()
		return io.ReadAll(bodyReader)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	return body, nil
}

// getStreamWriter returns the appropriate stream writer for the current request.
func (ra *relayAttempt) getStreamWriter() StreamWriter {
	if ra.streamWriter != nil {
		return ra.streamWriter
	}
	return ra.c.Writer
}

// applyParamOverride merges channel-level JSON request overrides and records the final upstream payload.
func (ra *relayAttempt) applyParamOverride(outboundRequest *http.Request) error {
	if err := helper.ApplyParamOverride(outboundRequest, ra.channel.ParamOverride); err != nil {
		return err
	}
	if requestBody, readErr := readOutboundRequestBody(outboundRequest); readErr == nil {
		ra.metrics.SetTransportRequestPayload(requestBody, ra.internalRequest.Model)
	}
	return nil
}

// copyHeaders 复制请求头，过滤 hop-by-hop 头
func (ra *relayAttempt) copyHeaders(outboundRequest *http.Request) {
	if ra.c != nil {
		for key, values := range ra.c.Request.Header {
			lowerKey := strings.ToLower(key)
			if hopByHopHeaders[lowerKey] {
				continue
			}
			// anthropic-beta 需要与出站默认值合并去重，避免覆盖掉
			// 透传路径预置的 prompt-caching / extended-cache-ttl 基线。
			if lowerKey == "anthropic-beta" {
				existing := outboundRequest.Header.Get(key)
				for _, value := range values {
					existing = mergeBetaHeader(existing, value)
				}
				if existing != "" {
					outboundRequest.Header.Set(key, existing)
				}
				continue
			}
			for _, value := range values {
				outboundRequest.Header.Set(key, value)
			}
		}
	}
	if outboundRequest.Header.Get("User-Agent") == "" {
		outboundRequest.Header.Set("User-Agent", "")
	}
	if len(ra.channel.CustomHeader) > 0 {
		for _, header := range ra.channel.CustomHeader {
			outboundRequest.Header.Set(header.HeaderKey, header.HeaderValue)
		}
	}
}

// mergeBetaHeader 合并两个逗号分隔的 anthropic-beta 字段值，去重并保留先后顺序。
func mergeBetaHeader(existing, incoming string) string {
	seen := make(map[string]struct{}, 8)
	merged := make([]string, 0, 8)
	for _, source := range []string{existing, incoming} {
		for _, entry := range strings.Split(source, ",") {
			normalized := strings.TrimSpace(entry)
			if normalized == "" {
				continue
			}
			if _, ok := seen[normalized]; ok {
				continue
			}
			seen[normalized] = struct{}{}
			merged = append(merged, normalized)
		}
	}
	return strings.Join(merged, ",")
}

// sendRequest 发送 HTTP 请求
func (ra *relayAttempt) sendRequest(req *http.Request) (*http.Response, error) {
	httpClient, err := helper.ChannelHTTPClientWithContext(req.Context(), ra.channel)
	if err != nil {
		log.Warnf("failed to get http client: %v", err)
		return nil, err
	}

	req = ra.attachFirstTokenBudget(req)

	response, err := httpClient.Do(req)
	if err != nil {
		if timeoutErr := ra.firstTokenTimeoutIfNeeded(req.Context(), err); timeoutErr != nil {
			ra.closeFirstTokenBudget()
			return nil, timeoutErr
		}
		if isClientCancellation(req.Context(), err) {
			log.Infof("request canceled before upstream response: %v", err)
		} else {
			log.Warnf("failed to send request: %v", err)
		}
		ra.closeFirstTokenBudget()
		return nil, err
	}

	if response != nil && response.Body != nil && ra.firstTokenBudget != nil {
		response.Body = &closeWithFuncReadCloser{
			ReadCloser: response.Body,
			onClose:    ra.closeFirstTokenBudget,
		}
	}

	return response, nil
}

// handleStreamResponseV2 uses StreamProcessor for unified stream handling.
func (ra *relayAttempt) handleStreamResponseV2(ctx context.Context, response *http.Response) error {
	defer ra.closeFirstTokenBudget()

	// Content-Type validation
	if ct := response.Header.Get("Content-Type"); ct != "" && !strings.Contains(strings.ToLower(ct), "text/event-stream") {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 16*1024))
		return fmt.Errorf("upstream returned non-SSE content-type %q for stream request: %s", ct, string(body))
	}

	// Hand off early heartbeat
	ra.heartbeat.Hand()

	// Build transform function
	hold := newEmptyOutputHold(ra.emptyRetryEnabled)
	transform := hold.wrapTransform(ra)

	// Determine first token timeout
	var firstTokenTimeout time.Duration
	if ra.firstTokenTimeOutSec > 0 && ra.firstTokenBudget == nil {
		firstTokenTimeout = time.Duration(ra.firstTokenTimeOutSec) * time.Second
	}

	// Create StreamProcessor
	processor := stream.NewStreamProcessor(stream.StreamConfig{
		Source:            stream.NewSSESource(response.Body, maxSSEEventSize),
		Transform:         transform,
		Writer:            ra.getStreamWriter(),
		Context:           ctx,
		FirstTokenTimeout: firstTokenTimeout,
		HeartbeatInterval: streamHeartbeatInterval(),
		// OnHeldChunk 仅在开关启用时接线（保持 chunk 才应重排首字计时）；
		// OFF 时保持 nil，processor 的 held 分支整体短路，legacy 定时器语义不变。
		OnHeldChunk: holdOnHeldChunk(ra),
		OnFirstToken: func() {
			ra.metrics.SetFirstTokenTime(time.Now())
			ra.stopFirstTokenTimer()
		},
	})

	// Run processor
	err := processor.Run()

	// Track payload written for metrics collection
	if processor.PayloadWritten() {
		ra.streamPayloadWritten.Store(true)
	}

	// 流结束原因：进 RelayMetrics，relay.complete / empty_stream 行携带（G3）。
	ra.recordStreamEndReason(processor.EndReason())

	// 断连残余观测：保持中的字节未写客户端（Written=false → 可重试），
	// Run 返回后记录残余规模即可（Kleppmann 裁定：无需 cancel 分支守卫）。
	if hold.holding() && hold.heldBytes() > 0 {
		log.Debugf("empty-output hold discarded %d buffered bytes on stream end (written=%t)",
			hold.heldBytes(), processor.PayloadWritten())
	}

	// round-5 前置①（路径错位闭合）：transform 路径的流尾 usage 形态影子行
	// （observe-only，非扰动——见 emitTransformEmptyStreamUsageShadow 注释）。
	ra.emitTransformEmptyStreamUsageShadow(hold, processor.EndReason())

	// Handle first token timeout specifically
	if err != nil && strings.Contains(err.Error(), "first token timeout") {
		_ = response.Body.Close()
		return ra.firstTokenTimeoutError()
	}

	// Check for context cancellation with first token timeout
	if err != nil {
		if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, err); timeoutErr != nil {
			return timeoutErr
		}
	}

	return err
}

// handleStreamResponsePassthroughV2 uses StreamProcessor for unified passthrough handling.
// Works with any PassthroughCapable transformer (Anthropic, OpenAI Responses, etc.).
func (ra *relayAttempt) handleStreamResponsePassthroughV2(ctx context.Context, response *http.Response, cfg model.PassthroughConfig) error {
	defer ra.closeFirstTokenBudget()

	// Content-Type validation
	if ct := response.Header.Get("Content-Type"); ct != "" && !strings.Contains(strings.ToLower(ct), "text/event-stream") {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 16*1024))
		return fmt.Errorf("upstream returned non-SSE content-type %q for stream request: %s", ct, string(body))
	}

	// Hand off early heartbeat
	ra.heartbeat.Hand()

	// Determine first token timeout
	var firstTokenTimeout time.Duration
	if ra.firstTokenTimeOutSec > 0 && ra.firstTokenBudget == nil {
		firstTokenTimeout = time.Duration(ra.firstTokenTimeOutSec) * time.Second
	}

	// Buffer for raw stream (for metrics collection)
	var rawStreamBuf bytes.Buffer

	// G6 实验开关：passthrough hold-until-output-evidence（默认 OFF）。
	var ptHold *passthroughOutputHold
	var ptTransform stream.StreamTransform
	if emptyPassthroughHoldEnabled() {
		ptHold = newPassthroughOutputHold(cfg)
		ptTransform = func(_ context.Context, data []byte) ([]byte, error) {
			// RawSource 的 chunk 是 SSE 帧序列（"data: {...}\n\n"），不是单个 JSON；
			// 帧解析与逐帧决策封装在 hold 内（跨 chunk 半帧尾由 pending 缓冲，
			// flush-degrade 与保守放行契约见 passthroughOutputHold 注释）。
			return ptHold.transform(data)
		}
	}

	// Create StreamProcessor. Declared as a variable first so the OnFinish
	// closure below can query EndReason() for the stream_end_reason log field
	// (finalize assigns the reason before invoking OnFinish).
	var processor *stream.StreamProcessor
	processor = stream.NewStreamProcessor(stream.StreamConfig{
		Source:            stream.NewRawSource(response.Body, 32*1024),
		Transform:         ptTransform, // nil = pure passthrough (G6 off)
		Writer:            ra.getStreamWriter(),
		Context:           ctx,
		FirstTokenTimeout: firstTokenTimeout,
		HeartbeatInterval: streamHeartbeatInterval(),
		BufferRawStream:   true,
		TerminalEvents:    cfg.TerminalEvents,
		OnFirstToken: func() {
			ra.metrics.SetFirstTokenTime(time.Now())
			ra.stopFirstTokenTimer()
		},
		OnFinish: func(ctx context.Context, rawStream []byte) error {
			// 流尾观测分类与 usage 形态影子判别收敛到共享终态器（Nottingham：
			// 同一生命周期两个到达点，判定逻辑一处所有）；这里只补日志，不改变
			// 任何返回值与记账行为。分类在 safe.Go 之外同步执行，必须保持无 panic。
			ra.emitEmptyStreamFamily(rawStream, processor.EndReason())
			if len(rawStream) == 0 {
				return stream.ErrEmptyUpstreamStream
			}
			// Copy to buffer for metrics collection
			rawStreamBuf.Write(rawStream)

			// Collect passthrough metrics
			ra.collectPassthroughMetrics(ctx, rawStream)

			// Collect response if configured
			if cfg.CollectMetrics {
				ra.collectResponse()
			}

			log.Debugf("passthrough stream end")
			return nil
		},
	})

	// Run processor
	err := processor.Run()

	// Track payload written for metrics collection
	if processor.PayloadWritten() {
		ra.streamPayloadWritten.Store(true)
	}

	// 流结束原因：进 RelayMetrics，relay.complete / empty_stream 行携带（G3）。
	ra.recordStreamEndReason(processor.EndReason())

	// Handle first token timeout specifically
	if err != nil && strings.Contains(err.Error(), "first token timeout") {
		_ = response.Body.Close()
		return ra.firstTokenTimeoutError()
	}

	// Check for context cancellation with first token timeout
	if err != nil {
		if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, err); timeoutErr != nil {
			return timeoutErr
		}
	}

	// G6 终判：ErrEmptyUpstreamStream（finalize 零写入路径，OnFinish 未被调用）
	// 且全程保持中（零输出事件零放行）且 Suspect 终态帧在场 → 缺陷证据成立。
	// 客户端零字节（held 未写），statusCode=0 走既有同通道重试链（fresh clean
	// stream，无 dual created）。契约内缺失=breach（Responses completed 原生强制
	// usage），与 G4 transform 路径的差分为 written decision（见
	// passthroughOutputHold 注释）。
	//
	// round-5 开灯前置①：此路径 OnFinish 被饿死，relay.empty_stream 告警与
	// usage 形态影子行在修复前采不到。sawSuspect 闩锁把 void-prefix 后中途截断的
	// 流（EOF、无终态帧）挡在桶外（client_gone 免费标签规则：截断 ≠ 完成的空）；
	// 补打走共享终态器，heldRaw() 快照即完整流字节（pending 无后续可合并）。
	if err != nil && errors.Is(err, stream.ErrEmptyUpstreamStream) && ptHold != nil && ptHold.holding_() {
		if ptHold.suspect_() {
			ra.emitEmptyStreamFamily(ptHold.heldRaw(), stream.StreamEndReasonEmpty)
		}
		var channelID int
		if ra.channel != nil {
			channelID = ra.channel.ID
		}
		log.Warnw("relay.empty_stream_hold_failure",
			"start_time_unix", ra.metrics.StartTime.Unix(),
			"api_key_id", ra.apiKeyID,
			"group_id", ra.groupID,
			"channel_id", channelID,
			"channel", ra.channelNameForLog(),
			"model", ra.requestModel,
		)
		return err
	}

	// On disconnect with partial data, still try to collect metrics
	if err != nil && errors.Is(err, context.Canceled) && rawStreamBuf.Len() > 0 {
		ra.collectPassthroughMetrics(context.Background(), rawStreamBuf.Bytes())
		if cfg.CollectMetrics {
			ra.collectResponse()
		}
	}

	return err
}

// collectPassthroughMetrics parses raw SSE stream for metrics aggregation without mutating response.
func (ra *relayAttempt) collectPassthroughMetrics(ctx context.Context, rawStream []byte) {
	if len(rawStream) == 0 {
		return
	}

	// Try stream event adapter first (preferred)
	outEventAdapter, outOk := ra.outAdapter.(model.OutboundStreamEventTransformer)
	inEventAdapter, inOk := ra.inAdapter.(model.InboundStreamEventTransformer)
	if outOk && inOk {
		readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
		for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
			if err != nil {
				log.Debugf("passthrough metrics parse skipped: %v", err)
				return
			}
			if events, terr := outEventAdapter.TransformStreamEvent(ctx, []byte(ev.Data)); terr == nil && len(events) > 0 {
				_, _ = inEventAdapter.TransformStreamEvents(ctx, events)
			}
		}
		return
	}

	// Fallback to traditional stream transformer
	readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
	for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
		if err != nil {
			log.Debugf("passthrough metrics parse skipped: %v", err)
			return
		}
		if chunk, terr := ra.outAdapter.TransformStream(ctx, []byte(ev.Data)); terr == nil && chunk != nil {
			_, _ = ra.inAdapter.TransformStream(ctx, chunk)
		}
	}
}

// transformStreamData 转换流式数据
func (ra *relayAttempt) transformStreamData(ctx context.Context, data string) ([]byte, error) {
	events, ok, err := ra.decodeOutboundStreamEvents(ctx, []byte(data))
	if err != nil {
		log.Warnf("failed to transform stream events: %v", err)
		return nil, err
	}
	if ok {
		return ra.encodeInboundStreamEvents(ctx, events)
	}

	internalStream, err := ra.decodeOutboundStreamResponse(ctx, []byte(data))
	if err != nil {
		log.Warnf("failed to transform stream: %v", err)
		return nil, err
	}
	if internalStream == nil {
		return nil, nil
	}

	return ra.encodeInboundStreamResponse(ctx, internalStream)
}

func (ra *relayAttempt) decodeOutboundStreamEvents(ctx context.Context, data []byte) ([]model.StreamEvent, bool, error) {
	outEventAdapter, ok := ra.outAdapter.(model.OutboundStreamEventTransformer)
	if !ok {
		return nil, false, nil
	}
	if _, ok := ra.inAdapter.(model.InboundStreamEventTransformer); !ok {
		return nil, false, nil
	}
	events, err := outEventAdapter.TransformStreamEvent(ctx, data)
	if err != nil {
		return nil, true, err
	}
	return events, true, nil
}

func (ra *relayAttempt) encodeInboundStreamEvents(ctx context.Context, events []model.StreamEvent) ([]byte, error) {
	if len(events) == 0 {
		return nil, nil
	}
	inEventAdapter, ok := ra.inAdapter.(model.InboundStreamEventTransformer)
	if !ok {
		return nil, nil
	}
	inStream, err := inEventAdapter.TransformStreamEvents(ctx, events)
	if err != nil {
		log.Warnf("failed to transform inbound stream events: %v", err)
		return nil, err
	}
	return inStream, nil
}

func (ra *relayAttempt) decodeOutboundStreamResponse(ctx context.Context, data []byte) (*model.InternalLLMResponse, error) {
	return ra.outAdapter.TransformStream(ctx, data)
}

func (ra *relayAttempt) encodeInboundStreamResponse(ctx context.Context, internalStream *model.InternalLLMResponse) ([]byte, error) {
	inStream, err := ra.inAdapter.TransformStream(ctx, internalStream)
	if err != nil {
		log.Warnf("failed to transform stream: %v", err)
		return nil, err
	}
	return inStream, nil
}

// handleResponse 处理非流式响应
func (ra *relayAttempt) handleResponse(ctx context.Context, response *http.Response) error {
	internalResponse, err := ra.outAdapter.TransformResponse(ctx, response)
	if err != nil {
		log.Warnf("failed to transform response: %v", err)
		return fmt.Errorf("failed to transform outbound response: %w", err)
	}

	// 空输出检测（issue #155 端口）：上游返回 200 但所有 choice 均无可见内容。
	// 不依赖 CompletionTokens 判断——推理模型可能 CompletionTokens > 0 但无可见内容。
	// 返回 errEmptyOutput → StatusCode=0 → 走既有同通道重试链（与流式路径一致）。
	if ra.emptyRetryEnabled && isEmptyOutputResponse(internalResponse) {
		log.Infof("channel %s returned empty output (no visible content), will retry", ra.channelNameForLog())
		return errEmptyOutput
	}

	inResponse, err := ra.inAdapter.TransformResponse(ctx, internalResponse)
	if err != nil {
		log.Warnf("failed to transform response: %v", err)
		return fmt.Errorf("failed to transform inbound response: %w", err)
	}

	ra.c.Data(http.StatusOK, "application/json", inResponse)
	return nil
}

// collectResponse 收集响应信息
func (ra *relayAttempt) collectResponse() {
	if ra == nil || ra.inAdapter == nil || ra.metrics == nil {
		return
	}
	if !ra.responseCollected.CompareAndSwap(false, true) {
		return
	}
	internalResponse, err := ra.inAdapter.GetInternalResponse(ra.requestContext())
	if err != nil {
		log.Debugf("collectResponse: failed to get internal response: %v", err)
		return
	}
	if internalResponse == nil {
		log.Debugf("collectResponse: internal response is nil (stream may not be complete)")
		return
	}

	actualModel := strings.TrimSpace(internalResponse.Model)
	if actualModel == "" && ra.internalRequest != nil {
		actualModel = strings.TrimSpace(ra.internalRequest.Model)
	}
	ra.metrics.SetInternalResponse(internalResponse, actualModel)
}

// classifyPassthroughStreamEnd 对直通缓存流的结尾做纯分类，绝不修改流内容：
//   - empty         流中没有任何可解析事件（含仅有注释/空行）
//   - error_event   出现错误事件（类型 ∈ errorEvents，或 data 载荷为协议错误形状）
//   - terminal      出现协议终态事件（类型 ∈ terminalEvents）
//   - truncated     已解析出事件但流解析中途失败（含末尾事件被截断、事件超长）
//   - unclassified  解析失败 / 以上皆不匹配
//
// error_event 优先于 terminal：部分错误事件（如 response.failed）同时位于
// TerminalEvents 中，按终态处理会把上游失败当成正常完成。解析失败不猜测、
// 不 panic（OnFinish 在 safe.Go 之外执行）。重写自原 streamReachedTerminalEvent。
func classifyPassthroughStreamEnd(rawStream []byte, terminalEvents, errorEvents, voidPrefixEvents map[string]struct{}) string {
	kind, _ := classifyPassthroughStreamEndWithEvidence(rawStream, terminalEvents, errorEvents, voidPrefixEvents)
	return kind
}

// classifyPassthroughStreamEndWithEvidence 在分类之外报告该流是否携带输出事件
// （类型 ∉ terminalEvents ∪ errorEvents ∪ voidPrefixEvents 的非空事件——对
// Responses 即 response.output_item.added、任何 *.delta、response.output_text.done
// 等；对 Anthropic 即 content_block_start、任何 delta 事件）。
//
// 这是事件信封层的协议形状检查，不是内容启发式：终态只证明流未中断，不证明生成
// 发生过——上游桥把 429 洗成 created→completed 空壳流时，中间不存在任何输出事件。
// void-prefix 元事件（协议在 VoidPrefixEvents 声明的无输出语义事件——批次二③
// 收敛点，替代此前硬编码的 created/in_progress）不计为证据。解析失败时不报告
// 证据（false），与 kind=truncated/unclassified 的「不猜测」一致。
func classifyPassthroughStreamEndWithEvidence(rawStream []byte, terminalEvents, errorEvents, voidPrefixEvents map[string]struct{}) (string, bool) {
	if len(rawStream) == 0 {
		return passthroughStreamEmpty, false
	}
	readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
	eventCount := 0
	sawTerminal := false
	sawOutputEvent := false
	for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
		if err != nil {
			// 中段解析失败：已看到终态视为完整流；有事件但未到终态视为截断；
			// 一个事件都没解析出来则无法判断。
			if sawTerminal {
				return passthroughStreamTerminal, sawOutputEvent
			}
			if eventCount > 0 {
				return passthroughStreamTruncated, false
			}
			return passthroughStreamUnclassified, false
		}

		typ := strings.TrimSpace(ev.Type)
		var probe struct {
			Type  string          `json:"type"`
			Error json.RawMessage `json:"error"`
		}
		_ = json.Unmarshal([]byte(ev.Data), &probe)
		if typ == "" {
			typ = strings.TrimSpace(probe.Type)
		}

		if _, ok := errorEvents[typ]; ok {
			return passthroughStreamErrorEvent, sawOutputEvent
		}
		// 未类型化的顶层 error 字段是 OpenAI 系的事实错误形状（协议错误形状检查，
		// 非内容启发式）；显式 null 视为无错误。
		if len(probe.Error) > 0 && string(probe.Error) != "null" {
			return passthroughStreamErrorEvent, sawOutputEvent
		}
		if _, ok := terminalEvents[typ]; ok {
			// 不立即返回：后续事件中的错误事件应胜出终态。
			sawTerminal = true
		} else if typ != "" {
			if _, ok := voidPrefixEvents[typ]; !ok {
				sawOutputEvent = true
			}
		}
		eventCount++
	}
	if eventCount == 0 {
		return passthroughStreamEmpty, false
	}
	if sawTerminal {
		return passthroughStreamTerminal, sawOutputEvent
	}
	return passthroughStreamUnclassified, false
}

// classifyPassthroughStreamEnd 的返回值。relay.empty_stream 日志以该值为
// empty_stream_kind 字段，便于按缺陷族聚合检索。
const (
	passthroughStreamEmpty        = "empty"
	passthroughStreamErrorEvent   = "error_event"
	passthroughStreamTerminal     = "terminal"
	passthroughStreamTruncated    = "truncated"
	passthroughStreamUnclassified = "unclassified"
)

// isPassthroughEmptyStreamKind 报告该分类是否需要以 Warnw 级别记录。
// terminal 在调用侧按输出证据细分（terminal 且零输出事件 → terminal_no_output）；
// unclassified 无法定论，不打扰告警。
func isPassthroughEmptyStreamKind(kind string) bool {
	switch kind {
	case passthroughStreamEmpty, passthroughStreamErrorEvent, passthroughStreamTruncated:
		return true
	default:
		return false
	}
}

// shadowProbeSamplePct 影子探针采样率（百分比，0-100）。1-5% 的真重试样本购买
// absent 形态的混淆矩阵（Kleppmann 裁定：触发形态从不出现时，纯影子拿不到真值）。
// 本轮仅做采样标记（probe=true 进影子日志行），探针执行体（真重试 + 备份渠道
// replay）由 PR-3 ops panel 承载。0 = 关闭采样。
var shadowProbeSamplePct = 0

// shadowProbeSampled 确定性采样判定（FNV-1a over 稳定请求标识）。同一请求重放
// 日志时采样结论不漂移；边界内输入全拒（pct<=0）或全收（pct>=100）。
func shadowProbeSampled(pct int, seed string) bool {
	if pct <= 0 {
		return false
	}
	if pct >= 100 {
		return true
	}
	h := uint32(2166136261)
	for i := 0; i < len(seed); i++ {
		h ^= uint32(seed[i])
		h *= 16777619
	}
	return int(h%100) < pct
}

// logShadowEmptyRetry 记录影子判别命中（Infow 级，区别于告警——未改行为，非事故）。
// relay.empty_stream_shadow 形态字段：usage_zero（usage 在场且 output==0，判别式
// 触发形态）/ usage_absent（usage 缺失，NULL≠0，不触发，待实测桥的透传行为）。
// probe=true 标记本事件命中探针采样（执行体在 PR-3：真重试一次，log-only，
// 「重试产出可见内容」= 真阳性）。
func (ra *relayAttempt) logShadowEmptyRetry(usageForm string, channelID int, endReason stream.StreamEndReason) {
	fields := []interface{}{
		"usage_form", usageForm,
		"stream_end_reason", string(endReason),
		"start_time_unix", ra.metrics.StartTime.Unix(),
		"api_key_id", ra.apiKeyID,
		"group_id", ra.groupID,
		"channel_id", channelID,
		"channel", ra.channelNameForLog(),
		"model", ra.requestModel,
	}
	if shadowProbeSamplePct > 0 {
		seed := fmt.Sprintf("%d|%d|%d|%s", ra.apiKeyID, channelID, ra.metrics.StartTime.UnixNano(), ra.requestModel)
		fields = append(fields, "probe", shadowProbeSampled(shadowProbeSamplePct, seed))
	}
	log.Infow("relay.empty_stream_shadow", fields...)
}

// streamUsageOptionsInjected 报告本次 attempt 的上游请求是否被注入了
// stream_options.include_usage=true。注入发生在 ChatOutbound.TransformRequest 内部
// （chat-completions 流式出站，WS 与 HTTP 共用 attempt 链路），relay 层以协议形状
// 等价判断：chat 出站 + 内部请求为流式。仅服务影子观测（400 归因）。
func (ra *relayAttempt) streamUsageOptionsInjected() bool {
	if ra == nil || ra.outAdapter == nil || ra.internalRequest == nil {
		return false
	}
	if _, ok := ra.outAdapter.(*openaiOutbound.ChatOutbound); !ok {
		return false
	}
	return ra.internalRequest.Stream != nil && *ra.internalRequest.Stream
}

// emitEmptyStreamFamily 是空流缺陷族观测的共享终态器（Nottingham round-5 裁定：
// OnFinish 与 post-Run G6 终判是同一生命周期的两个到达点，判定逻辑必须收敛为
// 一处，而非三处打补丁）。输入 rawStream（流原始字节）与 endReason，补打：
//   - relay.empty_stream（Warnw，缺陷族告警，kind 细分 terminal_no_output）；
//   - relay.empty_stream_shadow（Infow，usage 形态影子判别，log-only）。
//
// 不修改任何返回值、记账与流字节——纯观测。调用方必须已确认零 payload 写入
// （OnFinish 到达点由 processor 的 finalize 保证；post-Run 到达点由
// ErrEmptyUpstreamStream 语义保证）。
func (ra *relayAttempt) emitEmptyStreamFamily(rawStream []byte, endReason stream.StreamEndReason) {
	terminalEvents, errorEvents, voidPrefixEvents := ra.passthroughEventSets()
	kind, hasOutputEvent := classifyPassthroughStreamEndWithEvidence(rawStream, terminalEvents, errorEvents, voidPrefixEvents)
	var channelID int
	if ra.channel != nil {
		channelID = ra.channel.ID
	}
	if isPassthroughEmptyStreamKind(kind) || (kind == passthroughStreamTerminal && !hasOutputEvent) {
		logKind := kind
		if kind == passthroughStreamTerminal {
			// created/in_progress/终态俱全却零输出事件：上游桥洗白失败
			// （如 429→200 空 completed 流）的信封签名，细分为独立缺陷族。
			logKind = "terminal_no_output"
		}
		log.Warnw("relay.empty_stream",
			"empty_stream_kind", logKind,
			"stream_end_reason", string(endReason),
			"start_time_unix", ra.metrics.StartTime.Unix(),
			"api_key_id", ra.apiKeyID,
			"group_id", ra.groupID,
			"channel_id", channelID,
			"channel", ra.channelNameForLog(),
			"model", ra.requestModel,
		)
	}
	// 影子判别器：终态 + 零输出事件（信封层零可见）时解析 usage 形态，
	// 记录「本来会重试」但不改变任何行为（不重试、不改返回值、不改记账）。
	// 毕业判据：zero 形态误报率≈0 后才允许该谓词管行为；absent 形态的
	// 处置由影子期实测桥的 usage 透传行为决定。
	if kind == passthroughStreamTerminal && !hasOutputEvent {
		switch observeEmptyStreamUsage(rawStream) {
		case emptyUsageZero:
			ra.logShadowEmptyRetry("usage_zero", channelID, endReason)
		case emptyUsageAbsent:
			ra.logShadowEmptyRetry("usage_absent", channelID, endReason)
		}
	}
}

// passthroughEventSets 提取 passthrough 判定所需的事件分类集合（终态/错误/
// void-prefix），供共享终态器的分类器使用。集合来自出站适配器的 PassthroughConfig。
// 归一化保证非 nil（分类器可安全查表，不必每次判空）。
func (ra *relayAttempt) passthroughEventSets() (terminalEvents, errorEvents, voidPrefixEvents map[string]struct{}) {
	terminalEvents = map[string]struct{}{}
	errorEvents = map[string]struct{}{}
	voidPrefixEvents = map[string]struct{}{}
	if ra.outAdapter == nil {
		return terminalEvents, errorEvents, voidPrefixEvents
	}
	pt, ok := ra.outAdapter.(model.PassthroughCapable)
	if !ok {
		return terminalEvents, errorEvents, voidPrefixEvents
	}
	cfg := pt.PassthroughConfig()
	if cfg.TerminalEvents != nil {
		terminalEvents = cfg.TerminalEvents
	}
	if cfg.ErrorEvents != nil {
		errorEvents = cfg.ErrorEvents
	}
	if cfg.VoidPrefixEvents != nil {
		voidPrefixEvents = cfg.VoidPrefixEvents
	}
	return terminalEvents, errorEvents, voidPrefixEvents
}
