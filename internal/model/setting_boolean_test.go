package model

import (
	"strings"
	"testing"
)

// 灯下演练（窗口零）前置 2:拼错的布尔值必须在 API 层大声拒绝(400),而不是
// 静默回退默认值。SettingKeyEmptyPassthroughHoldEnabled 属布尔校验白名单
//（setting.go 布尔 case）,本表锁定该契约,演练时只验证运维路径。

func TestSettingBooleanValidation(t *testing.T) {
	keys := []SettingKey{
		SettingKeyRelayLogKeepEnabled,
		SettingKeyResponsesWSEnabled,
		SettingKeyGroupHealthEnabled,
		SettingKeyOutlierRetireEnabled,
		SettingKeyWebDAVIncludeStats,
		SettingKeyEmptyPassthroughHoldEnabled,
	}

	for _, key := range keys {
		for _, value := range []string{"true", "false"} {
			s := Setting{Key: key, Value: value}
			if err := s.Validate(); err != nil {
				t.Fatalf("%s=%q must validate, got %v", key, value, err)
			}
		}
		for _, bad := range []string{"1", "0", "TRUE", "yes", "", " true", "true "} {
			s := Setting{Key: key, Value: bad}
			if err := s.Validate(); err == nil {
				t.Fatalf("%s=%q must be rejected loudly, got nil error", key, bad)
			} else if !strings.Contains(err.Error(), "true or false") {
				t.Fatalf("%s=%q rejection must say true or false, got %q", key, bad, err.Error())
			}
		}
	}
}
