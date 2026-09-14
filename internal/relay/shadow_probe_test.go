package relay

import "testing"

// G5 探针采样判定表测试：确定性（同种子同结论）、边界全拒/全收、分布合理。
func TestShadowProbeSampled(t *testing.T) {
	// 确定性：同输入同输出。
	seed := "3|75|1726302810194000000|gpt-5.6-sol"
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
