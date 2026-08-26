package site

import (
	"context"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/utils/log"
)

// VersionSegmentBackfill 一次性把存量站点账号按「投影期版本段语义」重投影，
// 让已在库里落盘的渠道 base_url 从「裸域名」修正为「补全版本段」的形态
// （custom/route_override 仍按用户原样透传）。已回填则跳过。
//
// 之所以放在 site 包而不是 op：op 被 sitesync 依赖，若 op 反向 import
// site/sitesync 会形成 op → site → sitesync → op 的循环依赖。site 包位于
// 依赖链顶层，可同时安全地调用 op 的 setting 接口与自身的 ProjectSite。
func VersionSegmentBackfill(ctx context.Context) {
	done, err := op.SettingGetBool(model.SettingKeyVersionSegmentBackfilled)
	if err == nil && done {
		return
	}

	startTime := time.Now()

	sites, err := op.SiteList(ctx)
	if err != nil {
		log.Errorf("version segment backfill: list sites failed: %v", err)
		return
	}

	if len(sites) == 0 {
		if err := op.SettingSetString(model.SettingKeyVersionSegmentBackfilled, "true"); err != nil {
			log.Errorf("version segment backfill: mark complete failed (no sites): %v", err)
			return
		}
		log.Infof("version segment backfill: no sites, marked complete")
		return
	}

	failures := 0
	for i := range sites {
		siteID := sites[i].ID
		if err := ProjectSite(ctx, siteID); err != nil {
			failures++
			log.Warnf("version segment backfill: re-project site %d failed: %v", siteID, err)
			continue
		}
	}

	if failures > 0 {
		// 有站点重投影失败：不标记完成，下次启动再重试。重投影是幂等的，
		// 重复执行不会破坏已有渠道。
		log.Warnf("version segment backfill: %d/%d sites failed, will retry next startup (took %s)",
			failures, len(sites), time.Since(startTime))
		return
	}

	if err := op.SettingSetString(model.SettingKeyVersionSegmentBackfilled, "true"); err != nil {
		log.Errorf("version segment backfill: mark complete failed: %v", err)
		return
	}
	log.Infof("version segment backfill done: re-projected %d sites in %s", len(sites), time.Since(startTime))
}
