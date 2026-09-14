package relay

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// G5 探针采样判定表测试：确定性（同种子同结论）、边界全拒/全收、分布合理。
// 复算契约：种子 = "api_key_id|channel_id|start_time_unix|model"，四成分全部
// 出现在 relay.empty_stream_shadow 日志行内，采样结论可从日志逐行重建。
// （round-5 P2：种子曾用 UnixNano，无日志对应物，确定性不可审计。）
func TestShadowProbeSampled(t *testing.T) {
	// 确定性：同输入同输出。种子样例即日志行的成分拼接。
	seed := "3|75|1789394242|gpt-5.6-sol"
	first := shadowProbeSampled(5, seed)
	for i := 0; i < 100; i++ {
		if shadowProbeSampled(5, seed) != first {
			t.Fatalf("sampling must be deterministic for seed %q", seed)
		}
	}

	// 边界：pct<=0 全拒，pct>=100 全收。
	seeds := []string{"", "a", "3|75|x", "zzzz", "3|21|0|gpt-4o"}
	for _, s := range seeds {
		if shadowProbeSampled(0, s) {
			t.Fatalf("pct=0 must reject %q", s)
		}
		if shadowProbeSampled(-1, s) {
			t.Fatalf("pct<0 must reject %q", s)
		}
		if !shadowProbeSampled(100, s) {
			t.Fatalf("pct=100 must accept %q", s)
		}
	}

	// 分布：5% 下 10k 种子命中数应落在 4%-6% 之间（宽松界，捕获系统性偏差）。
	hits := 0
	n := 10_000
	for i := 0; i < n; i++ {
		if shadowProbeSampled(5, seeds[0]+string(rune(i))) {
			hits++
		}
	}
	ratio := float64(hits) / float64(n)
	if ratio < 0.04 || ratio > 0.06 {
		t.Fatalf("5%% sampling drifted: got %.2f%% (%d/%d)", ratio*100, hits, n)
	}
}

// round-5 P2：400 归因日志的证据位截断表征——长 body 截到 300 字节、
// 多字节 rune 边界兜底有效 UTF-8、短 body 原样。
func TestTruncateUpstreamError(t *testing.T) {
	long := strings.Repeat("x", 400)
	got := truncateUpstreamError(long)
	if len(got) != 300 {
		t.Fatalf("long body must truncate to 300 bytes, got %d", len(got))
	}

	// 300 字节边界落在多字节 rune 中间：截断后必须仍是有效 UTF-8。
	mixed := strings.Repeat("a", 299) + "中中中"
	got = truncateUpstreamError(mixed)
	if !utf8.ValidString(got) {
		t.Fatalf("truncated body must stay valid UTF-8, got %q", got)
	}
	if got != strings.Repeat("a", 299) {
		t.Fatalf("partial rune must be dropped, got %q", got)
	}

	if got := truncateUpstreamError("short error"); got != "short error" {
		t.Fatalf("short body must pass through, got %q", got)
	}
}
