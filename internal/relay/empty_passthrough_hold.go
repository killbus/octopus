package relay

import (
	"bytes"
	"encoding/json"
	"strings"

	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
)

// emptyPassthroughHoldEnabled 读取 G6 实验开关（系统设置，默认 OFF：设置缺失时
// SettingGetBool 返回 false）。var 便于测试替换。
var emptyPassthroughHoldEnabled = func() bool {
	enabled, err := op.SettingGetBool(dbmodel.SettingKeyEmptyPassthroughHoldEnabled)
	if err != nil {
		return false
	}
	return enabled
}

// passthroughOutputHold 实现 passthrough 路径的 hold-until-output-evidence（G6，
// 实验性，默认 OFF——Group 无关，系统设置 SettingKeyEmptyPassthroughHoldEnabled）。
//
// Nottingham 裁定的适用边界：全量 hold 让健康路径为 created 交付付 TTFT；本机制
// 只缓冲 void-prefix 元事件（response.created / response.in_progress），任何输出
// 事件到达即 flush 全部并永久直通——健康路径的代价收敛为「created 字节延迟到首个
// 输出事件」，输出事件之后零干预。事故形态（429→200 空壳流：created→completed、
// 零输出事件、零 usage）保持到流尾，由 OnFinish 判 failure → 客户端零字节 →
// ErrEmptyUpstreamStream 走既有同通道重试链（fresh clean stream，无 dual created）。
//
// 谓词与 G4 transform 路径的差分（硬约束⑦，written decision）：本谓词把 usage
// 缺失计为 failure——Responses 协议的 response.completed 原生强制 usage 契约
// （research：OpenAI Responses schema mandates usage），缺失即契约违反；transform
// 路径面向多协议（含无契约的 chat-completions 桥），维持 unknown 不触发。
// TTFB 语义：保持期间 payloadWritten 不置位，首字时间反映首个输出事件的交付时刻。
//
// 8 MiB 上限超限 → flush-degrade 降级为直通（held 字节是完整 SSE 帧，绝不截断，
// 与 maxEmptyOutputHoldBytes 同契约）。
type passthroughOutputHold struct {
	buf       bytes.Buffer // 持有的 void-prefix 原始字节（flush 时先行写入）
	pending   []byte       // 上一 chunk 的未完成 SSE 帧尾（与下一 chunk 合并解析）
	heldBytes int          // 持有的原始 chunk 字节（buf+pending 总量，cap 依据）
	holding   bool         // 是否仍在拦截（未 flush 放行）
	capped    bool         // 超限闩锁：永久降级直通
	// sawSuspect 闩锁（round-5 开灯前置①）：observe 在 Suspect 分支置位。post-Run
	// G6 终判以 holding_() && sawSuspect 为闸——仅 Suspect 终态帧在场才判
	// hold_failure 并补打影子行；void-prefix 后中途截断的流（EOF、无终态帧）不入
	// usage 形态桶（client_gone 免费标签规则：截断 ≠ 完成的空）。
	sawSuspect bool
}

func newPassthroughOutputHold() *passthroughOutputHold {
	return &passthroughOutputHold{holding: true}
}

// passthroughHoldAction 是 Transform 对单个上游 chunk 的处置决策。
type passthroughHoldAction int

const (
	// passthroughHoldKeep chunk 计入 void-prefix，继续持有（不写客户端）。
	passthroughHoldKeep passthroughHoldAction = iota
	// passthroughHoldRelease flush 全部持有字节 + 当前 chunk（合法流）。
	passthroughHoldRelease
	// passthroughHoldSuspect 终态且缺陷证据成立：chunk 一并持有到流尾，
	// 由 OnFinish 谓词终判（failure → ErrEmptyUpstreamStream）。
	passthroughHoldSuspect
)

// transform 是 Transform 的主体：用跨 chunk 帧缓冲解析每个 SSE 帧，逐帧交给
// observe 决策。聚合规则：任一帧 Release → 本 chunk 全部字节（含其前的 keep 帧
// 与半帧尾）随持有字节一次 flush——流顺序由原始字节序保证，且此后永久直通；
// 全部帧 Keep/Suspect → 帧字节并入 buf 继续持有，半帧尾缓存在 pending。
// cap 触发时 flush-degrade：持有字节 + 本 chunk 完整写出（绝不截断）。
func (h *passthroughOutputHold) transform(data []byte, terminalEvents, errorEvents map[string]struct{}) ([]byte, error) {
	if !h.holding || h.capped {
		return data, nil // 已直通
	}

	// 跨 chunk 帧组装：pending 尾 + 当前 chunk，按 SSE 帧边界切分。
	combined := append(h.pending, data...)
	h.pending = nil

	sawRelease := false
	rest := combined
	for len(rest) > 0 {
		var frame []byte
		if idx := bytes.Index(rest, []byte("\n\n")); idx >= 0 {
			frame = rest[:idx+2]
			rest = rest[idx+2:]
		} else if idx := bytes.Index(rest, []byte("\r\n\r\n")); idx >= 0 {
			frame = rest[:idx+4]
			rest = rest[idx+4:]
		} else {
			break // 半帧尾
		}
		if h.observe(sseFrameEventType(frame), frame, terminalEvents, errorEvents) == passthroughHoldRelease {
			sawRelease = true
		}
	}

	if sawRelease {
		// flush 点：输出/错误证据到达 → 持有字节 + 本 chunk 全部（含 keep 帧与
		// 半帧尾，原始顺序即流顺序）合并一次 Write，此后纯直通（Nottingham：
		// 健康路径代价收敛到此处）。
		return h.flushAllBytes(combined), nil
	}

	// Keep/Suspect：完整帧字节并入 buf，继续持有；半帧尾等下个 chunk。
	frames := combined[:len(combined)-len(rest)]
	if len(frames) > 0 {
		if !h.hold(frames) {
			// 8 MiB 超限：flush-degrade 直通（不截断）
			return h.flushAllBytes(combined), nil
		}
	}
	h.pending = append(h.pending, rest...)
	return nil, nil
}

// sseEventDataPayload 从单个 SSE 帧提取 data 载荷（多行 data 按协议拼接）。
// 无 data 行 → nil。
func sseEventDataPayload(frame []byte) []byte {
	var payload []byte
	for _, rawLine := range strings.Split(string(frame), "\n") {
		line := strings.TrimRight(rawLine, "\r")
		if strings.HasPrefix(line, "data:") {
			payload = append(payload, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ")...)
		}
	}
	return payload
}

// sseFrameEventType 从单个 SSE 帧提取事件类型（event: 行优先，退化到 data JSON
// 的 "type" 字段）。两者皆缺 / JSON 解析失败 → ""。
func sseFrameEventType(frame []byte) string {
	for _, rawLine := range strings.Split(string(frame), "\n") {
		line := strings.TrimRight(rawLine, "\r")
		if strings.HasPrefix(line, "event:") {
			if typ := strings.TrimSpace(strings.TrimPrefix(line, "event:")); typ != "" {
				return typ
			}
		}
	}
	var probe struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(sseEventDataPayload(frame), &probe) == nil {
		return probe.Type
	}
	return ""
}

// observeChunkPayload 归一化 observe 的输入：纯 JSON 直接用；SSE 帧剥出 data
// 载荷（observe 的单元表测试直接喂 JSON，处理器路径喂完整帧）。
func observeChunkPayload(data []byte) []byte {
	if trimmed := bytes.TrimSpace(data); len(trimmed) > 0 && trimmed[0] == '{' {
		return data
	}
	if payload := sseEventDataPayload(data); len(payload) > 0 {
		return payload
	}
	return nil
}

// observe 决策单个已解析 chunk。event 为空 / 不可解析时保守放行（flush-degrade，
// 与分类器的「不猜测」一致——保持中收到的任何含糊输入都不该演变成静默截断）。
func (h *passthroughOutputHold) observe(typ string, data []byte, terminalEvents, errorEvents map[string]struct{}) passthroughHoldAction {
	if !h.holding || h.capped {
		return passthroughHoldRelease
	}
	payload := observeChunkPayload(data)
	if typ == "" {
		// 未类型化 chunk：尝试从载荷提取 type；仍为空则保守放行。
		var probe struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(payload, &probe)
		typ = probe.Type
		if typ == "" {
			return passthroughHoldRelease
		}
	}
	if _, ok := errorEvents[typ]; ok {
		// 错误事件：flush（处置维持现状——载荷透传 + error_event 告警，G6 不改变）。
		return passthroughHoldRelease
	}
	if typ == "response.created" || typ == "response.in_progress" {
		// void-prefix 元事件：无输出语义（与分类器的证据判定同构），持有。
		return passthroughHoldKeep
	}
	if _, ok := terminalEvents[typ]; ok {
		// 终态块：解析本块 usage。合法空轮（output>0）→ 放行；output==0 或缺失
		// （契约内 breach）→ 保持到流尾，OnFinish 终判。
		usage := extractChunkOutputTokens(payload)
		if usage != nil && *usage > 0 {
			return passthroughHoldRelease
		}
		h.sawSuspect = true
		return passthroughHoldSuspect
	}
	// 其余非空类型：输出证据（output_item.added / *.delta / done 等）→ 放行并
	// 永久直通（由调用方在 Release 时置 holding=false）。
	return passthroughHoldRelease
}

// extractChunkOutputTokens 从单 chunk 载荷提取 output_tokens（chat 顶层 usage 或
// Responses 的 response.usage）。解析失败 / usage 缺失 → nil。
func extractChunkOutputTokens(data []byte) *int64 {
	var probe struct {
		Response *struct {
			Usage *struct {
				OutputTokens     *int64 `json:"output_tokens"`
				CompletionTokens *int64 `json:"completion_tokens"`
			} `json:"usage"`
		} `json:"response"`
		Usage *struct {
			OutputTokens     *int64 `json:"output_tokens"`
			CompletionTokens *int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(data, &probe) != nil {
		return nil
	}
	var usage *struct {
		OutputTokens     *int64 `json:"output_tokens"`
		CompletionTokens *int64 `json:"completion_tokens"`
	}
	if probe.Usage != nil {
		usage = probe.Usage
	} else if probe.Response != nil {
		usage = probe.Response.Usage
	}
	if usage == nil {
		return nil
	}
	if usage.OutputTokens != nil {
		return usage.OutputTokens
	}
	return usage.CompletionTokens
}

// hold 追加持有字节；超限返回 false（调用方 flush-degrade）。
func (h *passthroughOutputHold) hold(chunk []byte) bool {
	if h.heldBytes+len(chunk) > maxEmptyOutputHoldBytes {
		return false
	}
	h.buf.Write(chunk)
	h.heldBytes += len(chunk)
	return true
}

// flushAllBytes 返回持有字节与 payload 拼接后的完整 Write 载荷（完整 SSE 帧边界）
// 并永久放行。payload 为本次观察到的完整 chunk（含 keep 帧/半帧尾，原始顺序即流顺序）。
func (h *passthroughOutputHold) flushAllBytes(payload []byte) []byte {
	defer h.release()
	out := make([]byte, 0, h.buf.Len()+len(payload))
	out = append(out, h.buf.Bytes()...)
	return append(out, payload...)
}

// release 释放保持（合法流 / 降级）。
func (h *passthroughOutputHold) release() {
	h.holding = false
	h.heldBytes = 0
}

// holding_ 报告是否仍保持（OnFinish 终判依据：全程零输出事件且零放行）。
func (h *passthroughOutputHold) holding_() bool {
	return h.holding
}

// suspect_ 报告 Suspect 终态帧是否在场（round-5 前置①：post-Run 补打影子行的
// 闸门条件之一——截断流不入桶）。
func (h *passthroughOutputHold) suspect_() bool {
	return h.sawSuspect
}

// heldRaw 返回全部持有字节（buf + pending 半帧尾；只读快照，不改变保持状态）。
// post-Run 终判时流已结束、pending 不再有后续 chunk 可合并，快照即完整流字节。
func (h *passthroughOutputHold) heldRaw() []byte {
	out := make([]byte, 0, h.buf.Len()+len(h.pending))
	out = append(out, h.buf.Bytes()...)
	return append(out, h.pending...)
}
