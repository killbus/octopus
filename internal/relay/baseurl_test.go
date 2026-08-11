package relay

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"testing"
	"time"

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
		{"https://example.com/v1/?token=abc/", "https://example.com/v1?token=abc/"},
		{"https://example.com/v1%2F?token=x", "https://example.com/v1%2F?token=x"},
		{"http://UPSTREAM.io", "http://upstream.io"},
		{"", ""},
	}
	for _, c := range cases {
		if got := canonicalBaseURL(c.in); got != c.want {
			t.Errorf("canonicalBaseURL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestBaseURLKeyPreservesQueryIdentity(t *testing.T) {
	withSlash := baseURLKey("https://example.com/v1/?token=abc/")
	withoutSlash := baseURLKey("https://example.com/v1/?token=abc")
	if withSlash == withoutSlash {
		t.Fatalf("distinct raw queries must not share a base URL key")
	}
	ordered := baseURLKey("https://example.com/v1?a=1&b=2")
	reordered := baseURLKey("https://example.com/v1?b=2&a=1")
	if ordered == reordered {
		t.Fatalf("query parameter order is opaque endpoint identity and must not be normalized")
	}
	for _, name := range []string{"key", "token", "signature", "api_key"} {
		t.Run(name, func(t *testing.T) {
			raw := "https://example.com/v1?z=2&" + name + "=alpha&a=1"
			if got := canonicalBaseURL(raw); got != raw {
				t.Fatalf("canonicalBaseURL() changed opaque query for %q: got %q", name, got)
			}
			changed := "https://example.com/v1?z=2&" + name + "=beta&a=1"
			if baseURLKey(raw) == baseURLKey(changed) {
				t.Fatalf("query value for %q must participate in endpoint identity", name)
			}
		})
	}
}

func TestBaseURLCoolerBackoffSaturates(t *testing.T) {
	cooler := &baseURLFailoverCooler{states: make(map[string]*baseURLCoolState)}
	const channelID = 42
	const endpoint = "https://example.com/v1"
	for i := 0; i < 100; i++ {
		cooler.recordFailure(channelID, endpoint)
	}

	cooler.mu.Lock()
	state := *cooler.states[coolerKey(channelID, endpoint)]
	cooler.mu.Unlock()

	now := time.Now()
	if !state.skipUntil.After(now) {
		t.Fatalf("saturated cooldown must remain in the future: %v", state.skipUntil)
	}
	if state.skipUntil.After(now.Add(baseURLCoolMaxDelay + time.Second)) {
		t.Fatalf("cooldown exceeded max delay: %v", state.skipUntil.Sub(now))
	}
}

func TestBaseURLFailoverSignalMatrix(t *testing.T) {
	hardFailure := errors.New("upstream failed")
	tests := []struct {
		name         string
		status       int
		written      bool
		continuation bool
		want         bool
	}{
		{name: "connection", status: 0, want: true},
		{name: "server error", status: http.StatusInternalServerError, want: true},
		{name: "rate limit", status: http.StatusTooManyRequests, want: false},
		{name: "service unavailable", status: http.StatusServiceUnavailable, want: false},
		{name: "endpoint not found", status: http.StatusNotFound, want: true},
		{name: "unauthorized", status: http.StatusUnauthorized, want: false},
		{name: "written", status: http.StatusInternalServerError, written: true, want: false},
		{name: "continuation", status: http.StatusInternalServerError, continuation: true, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isBaseURLFailoverSignal(context.Background(), tt.status, hardFailure, tt.written, tt.continuation)
			if got != tt.want {
				t.Fatalf("isBaseURLFailoverSignal() = %t, want %t", got, tt.want)
			}
		})
	}
	if isBaseURLFailoverSignal(context.Background(), 0, nil, false, false) {
		t.Fatalf("nil error must not trigger failover")
	}
	if isBaseURLCoolableFailureSignal(context.Background(), http.StatusNotFound, hardFailure, false, false) {
		t.Fatalf("404 may fail over but must not cool the entire endpoint")
	}
	if !isBaseURLCoolableFailureSignal(context.Background(), http.StatusInternalServerError, hardFailure, false, false) {
		t.Fatalf("500 must remain an endpoint cooling signal")
	}
}

func TestWSPoolKeySeparatesBaseURLs(t *testing.T) {
	first := newWSPoolKey(1, 2, nil, baseURLKey("https://a.example.com/v1"))
	second := newWSPoolKey(1, 2, nil, baseURLKey("https://b.example.com/v1"))
	if first == second {
		t.Fatalf("different base URLs must not share a WS pool key")
	}
}

func TestResolveContinuationBaseURLUsesAffinityEndpoint(t *testing.T) {
	ctx := setupRelayTestDB(t)
	channel := &dbmodel.Channel{
		ID: 42,
		BaseUrls: []dbmodel.BaseUrl{
			{URL: "https://a.example.com/v1"},
			{URL: "https://b.example.com/v1"},
		},
	}
	usedKey := dbmodel.ChannelKey{ID: 7, ChannelID: channel.ID}
	scope := wsAffinityScope{APIKeyID: 1, GroupID: 2, RequestModel: "model", ResponseID: "resp_1"}
	if err := getWSAffinityStore().Set(ctx, scope, wsAffinityEntry{
		ChannelID:    channel.ID,
		ChannelKeyID: usedKey.ID,
		BaseURLKey:   baseURLKey(channel.BaseUrls[1].URL),
	}, time.Minute); err != nil {
		t.Fatalf("Set affinity failed: %v", err)
	}

	ra := &relayAttempt{
		relayRequest: &relayRequest{ctx: ctx, apiKeyID: 1, groupID: 2, requestModel: "model"},
		channel:      channel,
		usedKey:      usedKey,
	}
	if got := ra.resolveContinuationBaseURL(ctx, "resp_1"); got != channel.BaseUrls[1].URL {
		t.Fatalf("resolved continuation URL = %q, want %q", got, channel.BaseUrls[1].URL)
	}
	ra.usedKey.ID++
	if got := ra.resolveContinuationBaseURL(ctx, "resp_1"); got != "" {
		t.Fatalf("mismatched key must not reuse affinity URL, got %q", got)
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
		ID: 42,
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

func TestResolveBaseURLsRandomUsesAllCandidates(t *testing.T) {
	ch := newTestBaseURLChannel()
	ch.BaseUrlMode = dbmodel.BaseUrlModeRandom
	seen := make(map[string]bool)
	for i := 0; i < 200; i++ {
		got := resolveBaseURLs(ch)
		if len(got) != len(ch.BaseUrls) {
			t.Fatalf("random mode returned %d candidates, want %d", len(got), len(ch.BaseUrls))
		}
		seen[got[0].URL] = true
	}
	if len(seen) < 2 {
		t.Fatalf("random mode did not distribute first choice across candidates: %#v", seen)
	}
}
