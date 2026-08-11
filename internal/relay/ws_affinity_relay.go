package relay

import (
	"context"
	"strings"

	"github.com/bestruirui/octopus/internal/utils/log"
)

// resolveContinuationBaseURL 对 continuation 请求从 affinity 反查端点 URL。
// 契约：DB 存 baseURLKey（sha256(canonicalBaseURL)），反查 = 对渠道各候选 canonicalBaseURL
// 逐条哈希比对。channelID+keyID 全匹配才使用；BaseUrls 已变更或 entry 无 key 时回退（返回空串）。
func (ra *relayAttempt) resolveContinuationBaseURL(ctx context.Context, previousResponseID string) string {
	if ra == nil || ra.channel == nil || strings.TrimSpace(previousResponseID) == "" {
		return ""
	}
	scope := wsAffinityScope{
		APIKeyID:     ra.apiKeyID,
		GroupID:      ra.groupID,
		RequestModel: ra.requestModel,
		ResponseID:   previousResponseID,
	}
	entry, ok := getWSAffinityStore().Get(ctx, scope)
	if !ok || entry == nil {
		return ""
	}
	// channelID+keyID 全匹配才使用（避免读错端点的 URL 放大错误）
	if entry.ChannelID != ra.channel.ID || entry.ChannelKeyID != ra.usedKey.ID {
		return ""
	}
	if strings.TrimSpace(entry.BaseURLKey) == "" {
		return ""
	}
	for _, bu := range ra.channel.BaseUrls {
		if strings.TrimSpace(bu.URL) == "" {
			continue
		}
		if baseURLKey(bu.URL) == entry.BaseURLKey {
			return bu.URL
		}
	}
	return "" // BaseUrls 已变更，原端点不在候选集
}

// applyContinuationAffinity 在 forwardViaWS/HTTP 的 continuation 路径调用：
// 命中 affinity URL 则覆盖 ra.baseURL，使续会话精确回原端点（R3）。
// 未命中或 mismatch 保持现状（effectiveBaseURL 兜底），不改变原有语义。
func (ra *relayAttempt) applyContinuationAffinity(ctx context.Context) {
	previousID := currentPreviousResponseID(ra.internalRequest)
	if strings.TrimSpace(previousID) == "" {
		return
	}
	if url := ra.resolveContinuationBaseURL(ctx, previousID); url != "" {
		ra.baseURL = url
		ra.baseURLKey = baseURLKey(url)
	}
}

func clearContinuationAffinity(ctx context.Context, req *relayRequest) {
	if req == nil || req.internalRequest == nil {
		return
	}
	responseID := strings.TrimSpace(currentPreviousResponseID(req.internalRequest))
	if responseID == "" {
		return
	}

	deleteWSResponseConn(responseID)
	scope := wsAffinityScope{
		APIKeyID:     req.apiKeyID,
		GroupID:      req.groupID,
		RequestModel: req.requestModel,
		ResponseID:   responseID,
	}
	if err := getWSAffinityStore().Delete(ctx, scope); err != nil {
		log.Debugf("failed to delete invalid ws response affinity (apikey=%d, group=%d, request_model=%s, response_id=%s): %v", req.apiKeyID, req.groupID, req.requestModel, responseID, err)
	}
}
func (ra *relayAttempt) recordSuccessfulWSAffinity(pc *pooledConn) {
	if ra == nil || ra.metrics == nil || ra.metrics.InternalResponse == nil || pc == nil {
		return
	}
	responseID := strings.TrimSpace(ra.metrics.InternalResponse.ID)
	if responseID == "" {
		return
	}
	ttl := wsAffinityTTL(ra.groupSessionTTL)
	bindWSResponseConn(responseID, pc.id, ttl)
	if ra.apiKeyID <= 0 || ra.groupID <= 0 || strings.TrimSpace(ra.requestModel) == "" {
		return
	}
	scope := wsAffinityScope{
		APIKeyID:     ra.apiKeyID,
		GroupID:      ra.groupID,
		RequestModel: ra.requestModel,
		ResponseID:   responseID,
	}
	entry := wsAffinityEntry{
		ChannelID:     ra.channel.ID,
		ChannelKeyID:  ra.usedKey.ID,
		UpstreamModel: ra.internalRequest.Model,
		BaseURLKey:    pc.poolKey.baseURLKey,
	}
	ctx := ra.requestContext()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := getWSAffinityStore().Set(ctx, scope, entry, ttl); err != nil {
		log.Debugf("failed to persist ws response affinity (apikey=%d, group=%d, request_model=%s, response_id=%s): %v", ra.apiKeyID, ra.groupID, ra.requestModel, responseID, err)
	}
}
