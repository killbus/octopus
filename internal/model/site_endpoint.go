package model

import (
	"fmt"
	"net/url"
	"strings"
)

type SiteModelEndpointSource string

const (
	SiteModelEndpointSourceFollowSite SiteModelEndpointSource = "follow_site"
	SiteModelEndpointSourceCustom     SiteModelEndpointSource = "custom"
)

type SiteModelEndpointResolutionSource string

const (
	SiteModelEndpointResolutionFollowSite    SiteModelEndpointResolutionSource = "follow_site"
	SiteModelEndpointResolutionDefaultCustom SiteModelEndpointResolutionSource = "default_custom"
	SiteModelEndpointResolutionRouteOverride SiteModelEndpointResolutionSource = "route_override"
)

type SiteModelEndpointConfig struct {
	Default        SiteModelEndpointDefault `json:"default"`
	RouteOverrides []SiteRouteEndpointSet   `json:"route_overrides,omitempty"`
}

type SiteModelEndpointDefault struct {
	Source      SiteModelEndpointSource `json:"source"`
	EndpointSet *SiteEndpointSet        `json:"endpoint_set,omitempty"`
}

type SiteRouteEndpointSet struct {
	RouteType   SiteModelRouteType `json:"route_type"`
	EndpointSet SiteEndpointSet    `json:"endpoint_set"`
}

type SiteEndpointSet struct {
	BaseURLs    []SiteModelEndpoint `json:"base_urls"`
	BaseURLMode BaseUrlMode         `json:"base_url_mode"`
}

type SiteModelEndpoint struct {
	URL    string `json:"url"`
	Weight int    `json:"weight,omitempty"`
}

func FollowSiteModelEndpointConfig() SiteModelEndpointConfig {
	return SiteModelEndpointConfig{
		Default: SiteModelEndpointDefault{Source: SiteModelEndpointSourceFollowSite},
	}
}

func NormalizeSiteModelEndpointConfig(config SiteModelEndpointConfig) SiteModelEndpointConfig {
	result := SiteModelEndpointConfig{
		Default: SiteModelEndpointDefault{Source: SiteModelEndpointSource(strings.TrimSpace(string(config.Default.Source)))},
	}
	if result.Default.Source == "" {
		result.Default.Source = SiteModelEndpointSourceFollowSite
	}
	if config.Default.EndpointSet != nil {
		normalized := NormalizeSiteEndpointSet(*config.Default.EndpointSet)
		result.Default.EndpointSet = &normalized
	}
	if len(config.RouteOverrides) > 0 {
		result.RouteOverrides = make([]SiteRouteEndpointSet, 0, len(config.RouteOverrides))
		for _, override := range config.RouteOverrides {
			result.RouteOverrides = append(result.RouteOverrides, SiteRouteEndpointSet{
				RouteType:   SiteModelRouteType(strings.TrimSpace(string(override.RouteType))),
				EndpointSet: NormalizeSiteEndpointSet(override.EndpointSet),
			})
		}
	}
	return result
}

func NormalizeSiteEndpointSet(set SiteEndpointSet) SiteEndpointSet {
	result := SiteEndpointSet{BaseURLMode: set.BaseURLMode}
	if len(set.BaseURLs) == 0 {
		return result
	}
	result.BaseURLs = make([]SiteModelEndpoint, 0, len(set.BaseURLs))
	for _, endpoint := range set.BaseURLs {
		weight := endpoint.Weight
		if set.BaseURLMode != BaseUrlModeWeighted {
			weight = 0
		}
		result.BaseURLs = append(result.BaseURLs, SiteModelEndpoint{
			URL:    NormalizeSiteModelEndpointURL(endpoint.URL),
			Weight: weight,
		})
	}
	return result
}

// NormalizeSiteModelEndpointURL performs only storage-safe normalization.
// RawQuery remains byte-for-byte unchanged; only the path portion may lose
// trailing slashes.
func NormalizeSiteModelEndpointURL(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	boundary := len(value)
	if index := strings.IndexByte(value, '?'); index >= 0 && index < boundary {
		boundary = index
	}
	if index := strings.IndexByte(value, '#'); index >= 0 && index < boundary {
		boundary = index
	}
	return strings.TrimRight(value[:boundary], "/") + value[boundary:]
}

func ValidateSiteModelEndpointConfig(config SiteModelEndpointConfig) error {
	switch config.Default.Source {
	case SiteModelEndpointSourceFollowSite:
		if config.Default.EndpointSet != nil {
			return fmt.Errorf("follow_site model endpoint source must not include endpoint_set")
		}
	case SiteModelEndpointSourceCustom:
		if config.Default.EndpointSet == nil {
			return fmt.Errorf("custom model endpoint source requires endpoint_set")
		}
		if err := ValidateSiteEndpointSet(*config.Default.EndpointSet); err != nil {
			return fmt.Errorf("default model endpoint set is invalid: %w", err)
		}
	default:
		return fmt.Errorf("unsupported model endpoint source: %s", config.Default.Source)
	}

	seenRoutes := make(map[SiteModelRouteType]struct{}, len(config.RouteOverrides))
	for _, override := range config.RouteOverrides {
		if !IsProjectedSiteModelRouteType(override.RouteType) {
			return fmt.Errorf("model endpoint override has unsupported route type: %s", override.RouteType)
		}
		if _, exists := seenRoutes[override.RouteType]; exists {
			return fmt.Errorf("model endpoint override has duplicate route type: %s", override.RouteType)
		}
		seenRoutes[override.RouteType] = struct{}{}
		if err := ValidateSiteEndpointSet(override.EndpointSet); err != nil {
			return fmt.Errorf("model endpoint override for %s is invalid: %w", override.RouteType, err)
		}
	}
	return nil
}

func ValidateSiteEndpointSet(set SiteEndpointSet) error {
	if !set.BaseURLMode.Valid() {
		return fmt.Errorf("unsupported base url mode: %d", set.BaseURLMode)
	}
	if len(set.BaseURLs) == 0 {
		return fmt.Errorf("at least one model endpoint is required")
	}
	seenURLs := make(map[string]struct{}, len(set.BaseURLs))
	for _, endpoint := range set.BaseURLs {
		if endpoint.URL == "" {
			return fmt.Errorf("model endpoint url is required")
		}
		parsed, err := url.Parse(endpoint.URL)
		if err != nil {
			return fmt.Errorf("model endpoint url is invalid: %w", err)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return fmt.Errorf("model endpoint url must use http or https")
		}
		if parsed.Host == "" {
			return fmt.Errorf("model endpoint url must have a host")
		}
		if parsed.Fragment != "" {
			return fmt.Errorf("model endpoint url must not include a fragment")
		}
		if _, exists := seenURLs[endpoint.URL]; exists {
			return fmt.Errorf("duplicate model endpoint url: %s", endpoint.URL)
		}
		seenURLs[endpoint.URL] = struct{}{}
		if set.BaseURLMode == BaseUrlModeWeighted && endpoint.Weight <= 0 {
			return fmt.Errorf("weighted model endpoint requires a positive weight")
		}
	}
	return nil
}

func CloneSiteEndpointSet(set SiteEndpointSet) SiteEndpointSet {
	clone := SiteEndpointSet{BaseURLMode: set.BaseURLMode}
	if set.BaseURLs != nil {
		clone.BaseURLs = append([]SiteModelEndpoint(nil), set.BaseURLs...)
	}
	return clone
}

func ResolveSiteModelEndpointSet(config SiteModelEndpointConfig, routeType SiteModelRouteType, followSiteURL string) (SiteEndpointSet, SiteModelEndpointResolutionSource) {
	for _, override := range config.RouteOverrides {
		if override.RouteType == routeType {
			return CloneSiteEndpointSet(override.EndpointSet), SiteModelEndpointResolutionRouteOverride
		}
	}
	if config.Default.Source == SiteModelEndpointSourceCustom && config.Default.EndpointSet != nil {
		return CloneSiteEndpointSet(*config.Default.EndpointSet), SiteModelEndpointResolutionDefaultCustom
	}
	return SiteEndpointSet{
		BaseURLs:    []SiteModelEndpoint{{URL: EffectiveModelBaseURL(followSiteURL, routeType)}},
		BaseURLMode: BaseUrlModeDelay,
	}, SiteModelEndpointResolutionFollowSite
}

func SiteModelEndpointConfigFromLegacy(items []SiteRouteBaseURL) SiteModelEndpointConfig {
	config := FollowSiteModelEndpointConfig()
	seenRoutes := make(map[SiteModelRouteType]struct{}, len(items))
	for _, item := range NormalizeSiteRouteBaseURLs(items) {
		if _, exists := seenRoutes[item.RouteType]; exists {
			continue
		}
		seenRoutes[item.RouteType] = struct{}{}
		config.RouteOverrides = append(config.RouteOverrides, SiteRouteEndpointSet{
			RouteType: item.RouteType,
			EndpointSet: SiteEndpointSet{
				BaseURLs:    []SiteModelEndpoint{{URL: item.BaseURL}},
				BaseURLMode: BaseUrlModeDelay,
			},
		})
	}
	return config
}

func SiteRouteBaseURLsFromModelEndpointConfig(config SiteModelEndpointConfig) []SiteRouteBaseURL {
	result := make([]SiteRouteBaseURL, 0, len(config.RouteOverrides))
	for _, override := range config.RouteOverrides {
		if len(override.EndpointSet.BaseURLs) == 0 {
			continue
		}
		result = append(result, SiteRouteBaseURL{
			RouteType: override.RouteType,
			BaseURL:   override.EndpointSet.BaseURLs[0].URL,
		})
	}
	return result
}

func LegacySiteRouteBaseURLsEquivalent(left, right []SiteRouteBaseURL) bool {
	leftMap := legacySiteRouteBaseURLMap(left)
	rightMap := legacySiteRouteBaseURLMap(right)
	if len(leftMap) != len(rightMap) {
		return false
	}
	for routeType, baseURL := range leftMap {
		if rightMap[routeType] != baseURL {
			return false
		}
	}
	return true
}

func legacySiteRouteBaseURLMap(items []SiteRouteBaseURL) map[SiteModelRouteType]string {
	result := make(map[SiteModelRouteType]string, len(items))
	for _, item := range NormalizeSiteRouteBaseURLs(items) {
		if _, exists := result[item.RouteType]; exists {
			continue
		}
		result[item.RouteType] = item.BaseURL
	}
	return result
}

func HasSiteModelEndpointOverrides(config SiteModelEndpointConfig) bool {
	return len(config.RouteOverrides) > 0
}
