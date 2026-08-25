package model

import (
	"strings"
)

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
