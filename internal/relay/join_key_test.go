package relay

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/relay/stream"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/bestruirui/octopus/internal/utils/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// 灯下演练④（round-5 走查裁定，Feathers/Kleppmann 合裁）：影子行的
// start_time_unix 与 RelayLog 的 Time 字段今天恰好同源（均
// metrics.StartTime.Unix()），但等式无测试钉住——「最易腐化的缝恰好无锁」。
// 本表锁定三条契约：
//   1. 关联键等式：hold_failure 行与 shadow 行的 start_time_unix 都等于
//      metrics.StartTime.Unix()（改毫秒/收尾时刻 → 红）；
//   2. 四元组消歧在场：api_key_id / channel_id / model 与 attempt 一致
//      （秒级撞键时手册④的消歧依据）；
//   3. hold_failure 与 shadow 同键（演练③引用的正确日志行）。

func fieldsToMap(fields []zapcore.Field) map[string]string {
	m := make(map[string]string, len(fields))
	for _, f := range fields {
		switch f.Type {
		case zapcore.StringType:
			m[f.Key] = f.String
		case zapcore.Int64Type, zapcore.Int32Type, zapcore.Int16Type, zapcore.Int8Type,
			zapcore.Uint64Type, zapcore.Uint32Type, zapcore.Uint16Type, zapcore.Uint8Type:
			m[f.Key] = strconv.FormatInt(f.Integer, 10)
		default:
			m[f.Key] = f.String
		}
	}
	return m
}

func TestShadowLogJoinKeyMatchesRelayLogTime(t *testing.T) {
	enablePassthroughHoldForTest(t)
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIResponse, transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	observedCore, observed := observer.New(zapcore.InfoLevel)
	prevLogger := log.Logger
	log.Logger = zap.New(observedCore).Sugar()
	defer func() { log.Logger = prevLogger }()

	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_join","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_join","object":"response","model":"gpt-4o","created_at":0,"output":[],"status":"completed"}}`,
		"",
		"",
	}, "\n")

	if err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(body), ra.ptCfg()); !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("shell stream must reach G6 final verdict, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("shell stream must stay held, got %q", recorder.Body.String())
	}

	wantStart := strconv.FormatInt(ra.metrics.StartTime.Unix(), 10)

	holdEntries := observed.FilterMessage("relay.empty_stream_hold_failure").All()
	if len(holdEntries) != 1 {
		t.Fatalf("expected exactly one relay.empty_stream_hold_failure warn, got %d", len(holdEntries))
	}
	hold := fieldsToMap(holdEntries[0].Context)
	if hold["start_time_unix"] != wantStart {
		t.Fatalf("hold_failure start_time_unix = %q, want %q (metrics.StartTime.Unix)", hold["start_time_unix"], wantStart)
	}

	shadowEntries := observed.FilterMessage("relay.empty_stream_shadow").All()
	if len(shadowEntries) != 1 {
		t.Fatalf("expected exactly one relay.empty_stream_shadow info, got %d", len(shadowEntries))
	}
	shadow := fieldsToMap(shadowEntries[0].Context)
	if shadow["start_time_unix"] != wantStart {
		t.Fatalf("shadow start_time_unix = %q, want %q (metrics.StartTime.Unix)", shadow["start_time_unix"], wantStart)
	}

	// 四元组消歧能力：秒级撞键时手册④的消歧依据必须逐字段在场。
	if shadow["api_key_id"] != strconv.Itoa(ra.apiKeyID) {
		t.Fatalf("shadow api_key_id = %q, want %q", shadow["api_key_id"], strconv.Itoa(ra.apiKeyID))
	}
	if shadow["model"] != ra.requestModel {
		t.Fatalf("shadow model = %q, want %q", shadow["model"], ra.requestModel)
	}
	// 测试 attempt 未设置 channel；生产打点对 nil channel 有守卫
	//（relay.go 终判路径 var channelID int + nil 检查），零值在场即可。
	if shadow["channel_id"] != "0" {
		t.Fatalf("shadow channel_id = %q, want %q (nil channel guard)", shadow["channel_id"], "0")
	}
}
