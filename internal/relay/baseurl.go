package relay

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	dbmodel "github.com/bestruirui/octopus/internal/model"
)

func randomIntn(n int) int {
	if n <= 0 {
		return 0
	}
	return rand.Intn(n)
}

// canonicalBaseURL 规范化 baseUrl 用于池键/指纹：去首尾空白、去尾斜杠、统一 scheme/host 大小写。
// MVP 明确不做 query 排序规范化：url.Query().Encode() 会二次编码已有 %XX，且对签名类参数排序
// 会导致不同排序被判同桶、跨签名连接复用。query 全序规范化留作扩展点。
func canonicalBaseURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimRight(trimmed, "/")
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return trimmed
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	return parsed.String()
}

// baseURLKey 返回 canonicalBaseURL 的 sha256 指纹（64 字符 hex）。
// 内存池键、WS health/unsupported 键与 DB 持久化统一用该指纹（F4 定稿的量纲统一：
// 存 = hash(canonicalURL)；反查 = 对候选先 canonicalBaseURL 再 hash 比对）。
// 敏感 query（?token=/signature=）仅以指纹形式落盘，原值只在内存。
func baseURLKey(raw string) string {
	sum := sha256.Sum256([]byte(canonicalBaseURL(raw)))
	return hex.EncodeToString(sum[:])
}

// baseURLFailoverCooler 是 per-(channel, canonicalURL) 的进程内失败冷却表。
// 连续失败 → 指数退避 skipUntil；成功即清。全部候选冷却中时 resolveBaseUrl 会 fail-open 全试。
// 纯内存、不落库，与 wsChannelHealth 同构。键为 channelID + "\x00" + canonicalBaseURL。
type baseURLFailoverCooler struct {
	mu     sync.Mutex
	states map[string]*baseURLCoolState
}

type baseURLCoolState struct {
	consecutiveFailures int
	skipUntil           time.Time
}

var baseURLCooler = &baseURLFailoverCooler{states: make(map[string]*baseURLCoolState)}

const (
	baseURLCoolBaseDelay   = 5 * time.Second
	baseURLCoolMaxDelay    = 5 * time.Minute
	baseURLCoolMaxFailures = 3
)

func coolerKey(channelID int, canonicalURL string) string {
	return fmt.Sprintf("%d\x00%s", channelID, canonicalURL)
}

// cooled reports whether the endpoint should be skipped (failover context).
func (c *baseURLFailoverCooler) cooled(channelID int, canonicalURL string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, ok := c.states[coolerKey(channelID, canonicalURL)]
	if !ok {
		return false
	}
	return time.Now().Before(state.skipUntil)
}

// recordFailure 记录一次失败；连续失败达到阈值进入退避。
func (c *baseURLFailoverCooler) recordFailure(channelID int, canonicalURL string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := coolerKey(channelID, canonicalURL)
	state, ok := c.states[key]
	if !ok {
		state = &baseURLCoolState{}
		c.states[key] = state
	}
	state.consecutiveFailures++
	if state.consecutiveFailures >= baseURLCoolMaxFailures {
		delay := time.Duration(1<<(state.consecutiveFailures-baseURLCoolMaxFailures)) * baseURLCoolBaseDelay
		if delay > baseURLCoolMaxDelay {
			delay = baseURLCoolMaxDelay
		}
		state.skipUntil = time.Now().Add(delay)
	}
}

// recordSuccess 清除失败计数与退避。
func (c *baseURLFailoverCooler) recordSuccess(channelID int, canonicalURL string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.states, coolerKey(channelID, canonicalURL))
}

// resetForTest 清空冷却表（仅测试使用，避免全局状态在测试间污染）。
func (c *baseURLFailoverCooler) resetForTest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.states = make(map[string]*baseURLCoolState)
}

// baseURLCandidate 是一次 resolve 产出的端点候选。
type baseURLCandidate struct {
	URL          string // 原始 URL，用于真正建连/转发
	CanonicalURL string // 规范化 URL，用于池键
	Key          string // sha256 指纹，用于 DB affinity
}

// resolveBaseURLs 按渠道 base_url_mode 解析出有序候选列表（failover 模式过滤冷却中的端点）。
func resolveBaseURLs(channel *dbmodel.Channel) []baseURLCandidate {
	if channel == nil || len(channel.BaseUrls) == 0 {
		return nil
	}

	raw := make([]dbmodel.BaseUrl, 0, len(channel.BaseUrls))
	for _, bu := range channel.BaseUrls {
		if strings.TrimSpace(bu.URL) == "" {
			continue
		}
		raw = append(raw, bu)
	}
	if len(raw) == 0 {
		return nil
	}

	mode := channel.BaseUrlMode.Normalize()

	// 冷却过滤（仅 failover 模式跳过冷却端点；其余模式保持全候选）
	var active []dbmodel.BaseUrl
	if mode == dbmodel.BaseUrlModeFailover {
		active = make([]dbmodel.BaseUrl, 0, len(raw))
		for _, bu := range raw {
			if baseURLCooler.cooled(channel.ID, canonicalBaseURL(bu.URL)) {
				continue
			}
			active = append(active, bu)
		}
		if len(active) == 0 {
			active = raw // fail-open：全部冷却中则全试
		}
	} else {
		active = raw
	}

	switch mode {
	case dbmodel.BaseUrlModeRandom, dbmodel.BaseUrlModeWeighted:
		return weightedRandomCandidates(channel, active)
	default: // Delay 与 Failover：按 Delay 升序
		return delayOrderedCandidates(channel, active)
	}
}

func delayOrderedCandidates(channel *dbmodel.Channel, bu []dbmodel.BaseUrl) []baseURLCandidate {
	out := make([]baseURLCandidate, 0, len(bu))
	// 选择排序，稳定地按 Delay 升序（同 Delay 保持原顺序）
	remaining := append([]dbmodel.BaseUrl(nil), bu...)
	for len(remaining) > 0 {
		bestIdx := 0
		for i := 1; i < len(remaining); i++ {
			if remaining[i].Delay < remaining[bestIdx].Delay {
				bestIdx = i
			}
		}
		item := remaining[bestIdx]
		out = append(out, candidateFromBaseUrl(channel.ID, item))
		remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)
	}
	return out
}

func weightedRandomCandidates(channel *dbmodel.Channel, bu []dbmodel.BaseUrl) []baseURLCandidate {
	out := make([]baseURLCandidate, 0, len(bu))
	total := 0
	for _, item := range bu {
		w := item.Weight
		if w <= 0 {
			w = 1
		}
		total += w
	}
	remaining := append([]dbmodel.BaseUrl(nil), bu...)
	for len(remaining) > 0 {
		if total <= 0 {
			break
		}
		pick := randomIntn(total)
		acc := 0
		idx := 0
		for i, item := range remaining {
			w := item.Weight
			if w <= 0 {
				w = 1
			}
			acc += w
			if pick < acc {
				idx = i
				break
			}
			idx = i
		}
		item := remaining[idx]
		out = append(out, candidateFromBaseUrl(channel.ID, item))
		w := item.Weight
		if w <= 0 {
			w = 1
		}
		total -= w
		remaining = append(remaining[:idx], remaining[idx+1:]...)
	}
	return out
}

func candidateFromBaseUrl(channelID int, item dbmodel.BaseUrl) baseURLCandidate {
	canonical := canonicalBaseURL(item.URL)
	return baseURLCandidate{
		URL:          item.URL,
		CanonicalURL: canonical,
		Key:          baseURLKey(item.URL),
	}
}

// resolveSingleBaseURL 返回策略下的首个候选（非 failover 上下文），并附带 Key。
// continuation 场景由调用方传入固定端点，不经过策略选择。
func resolveSingleBaseURL(channel *dbmodel.Channel) baseURLCandidate {
	candidates := resolveBaseURLs(channel)
	if len(candidates) == 0 {
		return baseURLCandidate{}
	}
	return candidates[0]
}
