package relay

import (
	"encoding/hex"
	"testing"

	dbmodel "github.com/bestruirui/octopus/internal/model"
)

func TestCanonicalBaseURL(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"https://Example.COM/v1", "https://example.com/v1"},
		{"https://example.com/v1/", "https://example.com/v1"},
		{" https://example.com/v1 ", "https://example.com/v1"},
		{"https://example.com/v1?mode=x", "https://example.com/v1?mode=x"},
		{"http://UPSTREAM.io", "http://upstream.io"},
		{"", ""},
	}
	for _, c := range cases {
		if got := canonicalBaseURL(c.in); got != c.want {
			t.Errorf("canonicalBaseURL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestBaseURLKeyStableAndHashed(t *testing.T) {
	key1 := baseURLKey("https://Example.com/v1")
	key2 := baseURLKey("https://example.com/v1/")
	if key1 != key2 {
		t.Fatalf("baseURLKey not stable across canonical forms: %q vs %q", key1, key2)
	}
	if len(key1) != 64 {
		t.Fatalf("baseURLKey length = %d, want 64", len(key1))
	}
	if _, err := hex.DecodeString(key1); err != nil {
		t.Fatalf("baseURLKey not hex: %v", err)
	}
	if key1 == "https://example.com/v1" {
		t.Fatalf("baseURLKey must not be plaintext URL")
	}
}

func newTestBaseURLChannel() *dbmodel.Channel {
	return &dbmodel.Channel{
		ID:         42,
		BaseUrls: []dbmodel.BaseUrl{
			{URL: "https://a.example.com/v1", Delay: 1000},
			{URL: "https://b.example.com/v1", Delay: 500},
			{URL: "https://c.example.com/v1", Delay: 2000},
		},
	}
}

func TestResolveBaseURLsDelayOrder(t *testing.T) {
	ch := newTestBaseURLChannel()
	ch.BaseUrlMode = dbmodel.BaseUrlModeDelay
	got := resolveBaseURLs(ch)
	if len(got) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(got))
	}
	// delay=500 应排第一
	if got[0].URL != ch.BaseUrls[1].URL {
		t.Fatalf("expected delay-min URL first, got %s", got[0].URL)
	}
}

func TestResolveBaseURLsFailoverSkipsCooled(t *testing.T) {
	baseURLCooler.resetForTest()
	defer baseURLCooler.resetForTest()

	ch := newTestBaseURLChannel()
	ch.BaseUrlMode = dbmodel.BaseUrlModeFailover

	// 冷却第一条
	first := ch.BaseUrls[0].URL
	for i := 0; i < baseURLCoolMaxFailures; i++ {
		baseURLCooler.recordFailure(ch.ID, canonicalBaseURL(first))
	}

	got := resolveBaseURLs(ch)
	if len(got) != 2 {
		t.Fatalf("expected 2 candidates after cooling first, got %d", len(got))
	}
	for _, c := range got {
		if c.URL == first {
			t.Fatalf("cooled URL %s should be skipped", c.URL)
		}
	}

	// 冷却全部 → fail-open 全试
	for _, bu := range ch.BaseUrls {
		for i := 0; i < baseURLCoolMaxFailures; i++ {
			baseURLCooler.recordFailure(ch.ID, canonicalBaseURL(bu.URL))
		}
	}
	got = resolveBaseURLs(ch)
	if len(got) != 3 {
		t.Fatalf("expected fail-open full candidates, got %d", len(got))
	}
}

func TestResolveBaseURLsWeightedRandom(t *testing.T) {
	ch := newTestBaseURLChannel()
	ch.BaseUrlMode = dbmodel.BaseUrlModeWeighted
	// 权重 5:1:1：P(首个=URL0)≈71%，避免 100:1:1 下 "全部轮次都抽中加权 URL" 的偶发失败
	ch.BaseUrls[0].Weight = 5
	ch.BaseUrls[1].Weight = 1
	ch.BaseUrls[2].Weight = 1

	firstCount := 0
	const rounds = 200
	for i := 0; i < rounds; i++ {
		got := resolveBaseURLs(ch)
		if len(got) == 0 {
			t.Fatalf("no candidates")
		}
		if got[0].URL == ch.BaseUrls[0].URL {
			firstCount++
		}
	}
	if firstCount == 0 {
		t.Fatalf("high-weight URL never picked first in %d rounds", rounds)
	}
	if firstCount == rounds {
		t.Fatalf("high-weight URL always first; random distribution not exercised")
	}
	if firstCount < rounds/2 {
		t.Fatalf("high-weight URL picked first only %d/%d rounds; weighted random not effective", firstCount, rounds)
	}
}
