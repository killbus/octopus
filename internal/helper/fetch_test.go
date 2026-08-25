package helper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestFetchModelsUsesBrowserHeadersAndSummarizesHTMLError(t *testing.T) {
	observedUserAgent := ""
	observedAccept := ""
	observedAcceptLanguage := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedUserAgent = r.Header.Get("User-Agent")
		observedAccept = r.Header.Get("Accept")
		observedAcceptLanguage = r.Header.Get("Accept-Language")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`<!DOCTYPE html><html lang="en-US"><head><title>Just a moment...</title></head><body>blocked</body></html>`))
	}))
	defer server.Close()

	_, err := FetchModels(context.Background(), model.Channel{
		Type:     outbound.OutboundTypeOpenAIChat,
		BaseUrls: []model.BaseUrl{{URL: server.URL, Delay: 0}},
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "managed-key"}},
	})
	if err == nil {
		t.Fatalf("expected FetchModels to fail")
	}
	if !strings.Contains(err.Error(), "http 403: Just a moment...") {
		t.Fatalf("expected summarized HTML error, got %v", err)
	}
	if !strings.Contains(observedUserAgent, "Mozilla/5.0") {
		t.Fatalf("expected browser user-agent, got %q", observedUserAgent)
	}
	if observedAccept == "" {
		t.Fatalf("expected Accept header to be set")
	}
	if observedAcceptLanguage == "" {
		t.Fatalf("expected Accept-Language header to be set")
	}
}

func TestFetchGeminiModelsURL_BareDomain(t *testing.T) {
	var observedPath string
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if requestCount == 1 {
			observedPath = r.URL.Path
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/v1beta") {
			_, _ = w.Write([]byte(`{"models":[]}`))
		} else {
			_, _ = w.Write([]byte(`{"data":[]}`))
		}
	}))
	defer server.Close()

	_, err := FetchModels(context.Background(), model.Channel{
		Type:     outbound.OutboundTypeGemini,
		BaseUrls: []model.BaseUrl{{URL: server.URL, Delay: 0}},
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "test-key"}},
	})
	if err != nil {
		t.Fatalf("FetchModels returned error: %v", err)
	}
	if observedPath != "/v1beta/models" {
		t.Fatalf("expected path %q, got %q", "/v1beta/models", observedPath)
	}
}

func TestFetchOpenAIModelsURL_BareDomain(t *testing.T) {
	var observedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer server.Close()

	_, err := FetchModels(context.Background(), model.Channel{
		Type:     outbound.OutboundTypeOpenAIChat,
		BaseUrls: []model.BaseUrl{{URL: server.URL, Delay: 0}},
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "test-key"}},
	})
	if err != nil {
		t.Fatalf("FetchModels returned error: %v", err)
	}
	if observedPath != "/v1/models" {
		t.Fatalf("expected path %q, got %q", "/v1/models", observedPath)
	}
}
