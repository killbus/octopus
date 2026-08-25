package model

import (
	"net/url"
	"strings"
)

// DefaultVersionSegmentForRouteType returns the version segment that the
// outbound transformer for the given route type will fill when the base URL
// has no trailing version segment.
func DefaultVersionSegmentForRouteType(routeType SiteModelRouteType) string {
	if NormalizeSiteModelRouteType(routeType) == SiteModelRouteTypeGemini {
		return "/v1beta"
	}
	return "/v1"
}

// EffectiveModelBaseURL computes the final outbound base URL (including the
// version segment, normalised) that the relay will use for the given site
// base URL and route type. It is a pure function suitable for unit testing
// and for the Site API to return to the frontend.
func EffectiveModelBaseURL(baseURL string, routeType SiteModelRouteType) string {
	if baseURL == "" {
		return ""
	}
	segment := DefaultVersionSegmentForRouteType(routeType)
	return ResolveOutboundBaseURL(baseURL, segment)
}

// HasVersionSegment reports whether the path ends with a version segment: a
// path segment starting with `v` followed by a digit (e.g. /v1, /v1beta,
// /v2). Paths such as /viewer do not count. /v1x does count (the rest is
// alphanumeric). Nested paths like /openai/v1 count; /openai/viewer does
// not.
func HasVersionSegment(path string) bool {
	segment := strings.Trim(path, "/")
	if segment == "" {
		return false
	}
	parts := strings.Split(segment, "/")
	return isVersionSegment(parts[len(parts)-1])
}

func isVersionSegment(seg string) bool {
	if len(seg) < 2 || seg[0] != 'v' {
		return false
	}
	if seg[1] < '0' || seg[1] > '9' {
		return false
	}
	for _, c := range seg[2:] {
		if !isAlnum(c) {
			return false
		}
	}
	return true
}

func isAlnum(c rune) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// ResolveOutboundBaseURL normalises a raw base URL into the outbound base
// used by the relay: trailing slashes are trimmed, a missing trailing version
// segment is filled with defaultSegment, and duplicate trailing version
// segments are collapsed to the first one. Query and fragment are preserved.
func ResolveOutboundBaseURL(rawBaseURL, defaultSegment string) string {
	value := strings.TrimSpace(rawBaseURL)
	if value == "" {
		return ""
	}

	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" {
		return NormalizeSiteModelEndpointURL(value)
	}

	scheme := parsed.Scheme + "://"
	if parsed.User != nil {
		scheme += parsed.User.String() + "@"
	}
	scheme += parsed.Host

	rawQuery := parsed.RawQuery
	fragment := parsed.Fragment
	parsed.RawQuery = ""
	parsed.Fragment = ""

	path := parsed.Path
	if idx := strings.IndexByte(path, '?'); idx >= 0 {
		path = path[:idx]
	}
	if idx := strings.IndexByte(path, '#'); idx >= 0 {
		path = path[:idx]
	}
	path = strings.TrimRight(path, "/")

	if hasTrailingVersionSegment(path) {
		path = collapseTrailingVersionSegments(path)
	} else {
		path = path + defaultSegment
	}

	result := scheme + path
	if rawQuery != "" {
		result += "?" + rawQuery
	}
	if fragment != "" {
		result += "#" + fragment
	}
	return result
}

// hasTrailingVersionSegment reports whether the last path segment is a
// version segment.
func hasTrailingVersionSegment(path string) bool {
	segment := strings.Trim(path, "/")
	if segment == "" {
		return false
	}
	parts := strings.Split(segment, "/")
	return isVersionSegment(parts[len(parts)-1])
}

// collapseTrailingVersionSegments removes duplicate trailing version
// segments, keeping only the first one.
func collapseTrailingVersionSegments(path string) string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return path
	}
	segments := strings.Split(trimmed, "/")

	var firstVersion string
	for _, seg := range segments {
		if isVersionSegment(seg) {
			firstVersion = seg
			break
		}
	}

	if firstVersion == "" {
		return path
	}

	result := make([]string, 0, len(segments))
	seenFirst := false
	for _, seg := range segments {
		if isVersionSegment(seg) && seg == firstVersion {
			if seenFirst {
				continue
			}
			seenFirst = true
		}
		result = append(result, seg)
	}
	return "/" + strings.Join(result, "/")
}
