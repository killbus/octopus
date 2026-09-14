package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/bestruirui/octopus/internal/relay/stream"
	"github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/tmaxmax/go-sse"
)

// errEmptyOutput 标记上游返回 200 但没有任何可见内容的空输出（issue #155 端口）。
// 不依赖 CompletionTokens 判断——推理模型可能消耗大量推理 token 却不产出可见内容。
// 返回该错误使 StatusCode=0 → isRetryableStatus(0)=true → 走既有同通道重试链。
var errEmptyOutput = errors.New("upstream returned empty output (no visible content)")

// emptyUsageVerdict 影子判别器对「终态 + 零可见 + usage」三者的裁决分类。
//
// 合取谓词（empty-output-retry-audit.md 第三轮 Kleppmann 裁定 + 研究先例
// new-api ValidUsage）：terminal ∧ 零可见输出 ∧ usage 在场且 output==0 →
// 记账自证的缺陷（合法 reasoning-only 的 output_tokens ≥ 推理 token > 0）。
// usage 缺失是「未知」而非「零」（NULL≠0）：桥接通道可能剥离 usage，缺失形态
// 只单独计数、不参与触发——影子期实测桥的透传行为后再定。
type emptyUsageVerdict int

const (
	// emptyUsageAbsent 流的 completed 载荷不含 usage 字段（或终态载荷不可解析）。
	emptyUsageAbsent emptyUsageVerdict = iota
	// emptyUsageZero usage 在场且 output_tokens==0——缺陷的在场形态。
	emptyUsageZero
	// emptyUsagePositive usage 在场且 output_tokens>0——与零可见矛盾，判别器豁免
	// （不触发，保持观测）。
	emptyUsagePositive
)

// observeEmptyStreamUsage 从流尾终态载荷中提取 usage 形态。只读，不修改流。
// 逐事件扫描：取第一个含 usage 字段的终态/任意载荷（Responses 的 usage 挂在
// response.completed 的 response 对象上；chat completions 的最终 chunk 自带 usage）。
func observeEmptyStreamUsage(rawStream []byte) emptyUsageVerdict {
	if len(rawStream) == 0 {
		return emptyUsageAbsent
	}
	readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
	for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
		if err != nil {
			return emptyUsageAbsent
		}
		var probe struct {
			Response *struct {
				Usage *struct {
					OutputTokens *int64 `json:"output_tokens"`
				} `json:"usage"`
			} `json:"response"`
			Usage *struct {
				OutputTokens *int64 `json:"output_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal([]byte(ev.Data), &probe) != nil {
			continue
		}
		usage := probe.Usage
		if usage == nil && probe.Response != nil {
			usage = probe.Response.Usage
		}
		if usage == nil {
			continue
		}
		if usage.OutputTokens == nil {
			return emptyUsageAbsent
		}
		if *usage.OutputTokens == 0 {
			return emptyUsageZero
		}
		return emptyUsagePositive
	}
	return emptyUsageAbsent
}

// maxEmptyOutputHoldBytes 限制空输出保持缓冲的大小（8 MiB）。
// 持有量为解码单元对应的原始上游 chunk 字节数（编码字节的同量级代理，cap 的目的是
// 界定内存而非精确字节契约）；超限立即 flush-degrade 降级为直通（绝不截断）。
// fork 的无界 buffer 是缺陷不是特性（design.md 风险表）。var 便于测试注入小上限，
// 与 maxSSEEventSize（type.go）同模式。
var maxEmptyOutputHoldBytes = 8 * 1024 * 1024

// messageHasVisibleContent 判断一条消息是否携带客户端可见内容（fork PR #155 端口）。
// Content / MultipleContent / Refusal / ToolCalls / Images / Audio 计为可见；
// reasoning（ReasoningContent / Reasoning / ReasoningBlocks）不计——
// reasoning-only 流是空输出重试的目标场景。
// Refusal 与 Content 同层级（Message 独立字段，非 MessageContent）：非流式
// refusal-only 响应（TransformResponse 设 Message.Refusal，response.go）对客户端
// 可见，判空会假重试（审计裁定链：Refusal is client-visible → must count as
// visible）。同一检查经 choice.Delta 覆盖 chunk 路径的潜伏缺口。
func messageHasVisibleContent(msg *model.Message) bool {
	if msg == nil {
		return false
	}
	if msg.Content.Content != nil && strings.TrimSpace(*msg.Content.Content) != "" {
		return true
	}
	if msg.Refusal != "" {
		return true
	}
	if len(msg.Content.MultipleContent) > 0 {
		return true
	}
	if len(msg.ToolCalls) > 0 {
		return true
	}
	if len(msg.Images) > 0 {
		return true
	}
	if msg.Audio != nil {
		return true
	}
	return false
}

// streamChunkHasVisibleContent 判断流式 chunk 是否携带可见内容（over choice.Delta）。
func streamChunkHasVisibleContent(resp *model.InternalLLMResponse) bool {
	if resp == nil {
		return false
	}
	for _, choice := range resp.Choices {
		if messageHasVisibleContent(choice.Delta) {
			return true
		}
	}
	return false
}

// isEmptyOutputResponse 判断非流式响应是否为空输出（所有 choice 均无可见内容）。
// 无 Choices（embedding 响应）不算空输出，保持 fallback 语义。
func isEmptyOutputResponse(resp *model.InternalLLMResponse) bool {
	if resp == nil {
		return false
	}
	if len(resp.Choices) == 0 {
		return false
	}
	for _, choice := range resp.Choices {
		if messageHasVisibleContent(choice.Message) {
			return false
		}
	}
	return true
}

// chunkObservation 对已解码上游 chunk 的保持策略分类结果。
type chunkObservation struct {
	visible  bool // 携带客户端可见载荷（文本 / 工具调用）
	terminal bool // 携带 finish_reason（终态块）
}

// observeStreamChunk 分类响应路径（TransformStream）的解码 chunk。
func observeStreamChunk(resp *model.InternalLLMResponse) chunkObservation {
	var obs chunkObservation
	if resp == nil {
		return obs
	}
	for _, choice := range resp.Choices {
		if messageHasVisibleContent(choice.Delta) || messageHasVisibleContent(choice.Message) {
			obs.visible = true
		}
		if choice.FinishReason != nil {
			obs.terminal = true
		}
	}
	return obs
}

// observeStreamEvents 分类事件路径（TransformStreamEvent）的解码事件。
// 可见 = TextDelta（含 Refusal）/ ToolCallStart / ToolCallDelta；终态 = MessageStop。
func observeStreamEvents(events []model.StreamEvent) chunkObservation {
	var obs chunkObservation
	for _, ev := range events {
		switch ev.Kind {
		case model.StreamEventKindTextDelta, model.StreamEventKindToolCallStart, model.StreamEventKindToolCallDelta:
			obs.visible = true
		case model.StreamEventKindMessageStop:
			obs.terminal = true
		}
	}
	return obs
}

// heldUnit 一次保持的已解码上游 chunk——已经出站解码（观测所需；outAdapter 为
// attempt 级，重建自动隔离），但**尚未**送入入站适配器编码。
// eventPath 为 true 时携带 events，否则携带 stream，二者互斥。
//
// 为什么持有解码单元而不是已编码字节（硬约束①，R5 headless-stream 裁定）：
// 入站编码会推进 request 级 inAdapter 的状态机（hasResponseCreated / sequenceNumber /
// streamAggregator）。若对保持 chunk 提前编码，abandon 重试后该状态泄漏进下一次
// attempt——response.created 被抑制、序号跳变、上一 attempt 的 reasoning 复活，
// 客户端 SDK 累加器直接崩溃。持有解码单元使 inAdapter 在放弃路径上零接触。
type heldUnit struct {
	eventPath bool
	events    []model.StreamEvent
	stream    *model.InternalLLMResponse
}

// emptyOutputHold transform 路径的空输出保持状态（per-attempt，随闭包重建自动隔离）。
//
// 不可见 chunk（reasoning-only / keep-alive / usage-only）的解码单元暂存到 pending
// 并返回 nil（处理器跳过路径，payloadWritten 不置位）；可见 / 终态 chunk 到达时
// 按到达顺序编码全部保持单元 + 当前 chunk 并合并 flush。
//
// flush 时刻的编码安全性（Feathers 裁定的适用边界）：保持期间本 attempt 必然零写入
// （无可见内容才有保持），因此 flush 前的编码失败落在 Written()=false → 可重试集合内；
// 编码成功后的 flush 是首次客户端写。"编码失败发生在部分写之后" 的不可重试场景
// 在保持语义下结构性不存在。
//
// 8 MiB 上限：超限 latch capped 并立即 flush 全部（降级为直通，放弃重试，绝不截断）。
type emptyOutputHold struct {
	enabled    bool
	pending    []heldUnit // 按到达顺序的已解码未编码单元
	heldRaw    int        // 持有单元对应的原始 chunk 字节数（cap 依据）
	hasVisible bool       // 释放闩锁：可见内容到达或 finish_reason flush 后置位
	capped     bool       // 超限闩锁：永久降级为直通
}

func newEmptyOutputHold(enabled bool) *emptyOutputHold {
	return &emptyOutputHold{enabled: enabled}
}

// active 报告保持机制是否正在拦截 chunk（启用且未释放且未降级）。
func (h *emptyOutputHold) active() bool {
	return h != nil && h.enabled && !h.hasVisible && !h.capped
}

// hold 决定一个不可见 chunk 是否继续持有（cap 检查在入队前）。
// 返回 false 表示超限，调用方应立即 flush-degrade。
func (h *emptyOutputHold) hold(unit heldUnit, rawBytes int) bool {
	if h.heldRaw+rawBytes > maxEmptyOutputHoldBytes {
		return false
	}
	h.pending = append(h.pending, unit)
	h.heldRaw += rawBytes
	return true
}

// flush 按到达顺序编码全部保持单元 + 当前单元，拼接为一次 Write 的字节
// （flush 点 ①/④/⑥；拼接的每段都是完整 SSE 帧，不破坏帧边界）。
// 编码失败丢弃全部保持并返回错误（flush 点 ③ 变体：Written=false → 可重试）。
func (h *emptyOutputHold) flush(ctx context.Context, ra *relayAttempt, cur heldUnit) ([]byte, error) {
	units := h.pending
	h.pending = nil
	h.heldRaw = 0
	h.hasVisible = true // 释放闩锁：flush 后（无论成败）本 attempt 不再保持
	if cur.events != nil || cur.stream != nil {
		units = append(units, cur)
	}
	out := make([]byte, 0, 1024)
	for _, u := range units {
		var encoded []byte
		var err error
		if u.eventPath {
			encoded, err = ra.encodeInboundStreamEvents(ctx, u.events)
		} else {
			encoded, err = ra.encodeInboundStreamResponse(ctx, u.stream)
		}
		if err != nil {
			log.Warnf("failed to transform inbound stream: %v", err)
			h.drop() // 编码失败：丢弃保持，Written=false → 可重试
			return nil, err
		}
		out = append(out, encoded...)
	}
	return out, nil
}

// drop 丢弃持有缓冲（Written=false → 可重试）。
func (h *emptyOutputHold) drop() {
	if h == nil {
		return
	}
	h.pending = nil
	h.heldRaw = 0
}

// heldBytes 返回当前持有的原始 chunk 字节数（Run 返回后的残余观测用）。
func (h *emptyOutputHold) heldBytes() int {
	if h == nil {
		return 0
	}
	return h.heldRaw
}

// holding 报告是否仍在保持（未释放且未降级）——Run 返回后的残余观测用。
func (h *emptyOutputHold) holding() bool {
	return h.active()
}

// holdOnHeldChunk 返回 StreamConfig.OnHeldChunk 回调：仅当空输出重试启用时接线
// （保持 chunk 到达 → 模式拆分重排首字计时）。OFF 时返回 nil，processor 的 held
// 分支整体短路——legacy 跳过 chunk 保持既有定时器语义（OFF 等价，硬约束⑤ Off 分支）。
func holdOnHeldChunk(ra *relayAttempt) func() {
	if ra == nil || !ra.emptyRetryEnabled {
		return nil
	}
	return ra.rearmFirstTokenTimer
}

// wrapTransform 把保持语义包进 transformStreamData 外层。
//
// OFF 时（h.enabled=false）直接委托原 transformStreamData——字节级行为不变。
//
// ON 时每个上游事件：出站解码（观测必需；outAdapter attempt 级，无跨 attempt 泄漏）
// → 分类 → 不可见且保持中：暂存解码单元、跳过入站编码、返回 nil（处理器跳过路径，
// inAdapter 零接触——硬约束①）；可见 / 终态 / 超限：flush（编码全部保持单元 + 当前
// chunk，合并一次 Write）。与 transformStreamData 相同的子路径顺序（先事件路径后
// 响应路径），行为差异仅为持有/释放决策与编码时机（flush 前必然零写入，见 flush 注释）。
func (h *emptyOutputHold) wrapTransform(ra *relayAttempt) stream.StreamTransform {
	if h == nil || !h.enabled {
		return func(ctx context.Context, data []byte) ([]byte, error) {
			return ra.transformStreamData(ctx, string(data))
		}
	}
	return func(ctx context.Context, data []byte) ([]byte, error) {
		events, ok, err := ra.decodeOutboundStreamEvents(ctx, data)
		if err != nil {
			log.Warnf("failed to transform stream events: %v", err)
			h.drop() // transform 错误，丢弃持有（Written=false → 可重试）
			return nil, err
		}
		if ok {
			if len(events) == 0 {
				return nil, nil
			}
			obs := observeStreamEvents(events)
			unit := heldUnit{eventPath: true, events: events}
			if h.active() {
				if !obs.visible && !obs.terminal {
					if h.hold(unit, len(data)) {
						return nil, nil // 保持：不入站编码，inAdapter 零接触
					}
					h.capped = true // flush 点 ⑥：超限降级
				}
				// flush 点 ①/④/⑥：可见 / 终态 / 超限 → 编码全部 + 当前，合并 Write
				return h.flush(ctx, ra, unit)
			}
			return ra.encodeInboundStreamEvents(ctx, events)
		}

		internalStream, err := ra.decodeOutboundStreamResponse(ctx, data)
		if err != nil {
			log.Warnf("failed to transform stream: %v", err)
			h.drop() // transform 错误，丢弃持有（Written=false → 可重试）
			return nil, err
		}
		if internalStream == nil {
			return nil, nil
		}
		obs := observeStreamChunk(internalStream)
		unit := heldUnit{stream: internalStream}
		if h.active() {
			if !obs.visible && !obs.terminal {
				if h.hold(unit, len(data)) {
					return nil, nil
				}
				h.capped = true // flush 点 ⑥：超限降级
			}
			return h.flush(ctx, ra, unit)
		}
		return ra.encodeInboundStreamResponse(ctx, internalStream)
	}
}
