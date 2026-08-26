package site

import (
	"context"
	"path/filepath"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
)

func setupVersionSegmentBackfillTestDB(t *testing.T) context.Context {
	t.Helper()

	if dbpkg.GetDB() != nil {
		_ = dbpkg.Close()
	}

	dbPath := filepath.Join(t.TempDir(), "octopus-vs-backfill-test.db")
	if err := dbpkg.InitDB("sqlite", dbPath, false); err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	if err := op.InitCache(); err != nil {
		t.Fatalf("InitCache failed: %v", err)
	}
	t.Cleanup(func() {
		_ = dbpkg.Close()
	})

	return context.Background()
}

// createBackfillFixture seeds a site + account + tokens + one OpenAI Chat model,
// enough for ProjectSite to produce a follow_site channel.
func createBackfillFixture(t *testing.T, ctx context.Context) (*model.Site, *model.SiteAccount) {
	t.Helper()

	site := &model.Site{
		Name:     "Backfill Site",
		Platform: model.SitePlatformNewAPI,
		BaseURL:  "https://example.com",
		Enabled:  true,
	}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}

	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "Primary Account",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "site-access-token",
		Enabled:        true,
		AutoSync:       false,
		AutoCheckin:    false,
	}
	if err := op.SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}

	tokens := []model.SiteToken{
		{SiteAccountID: account.ID, Name: "primary", Token: "key-primary", GroupKey: model.SiteDefaultGroupKey, GroupName: model.SiteDefaultGroupKey, Enabled: true},
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&tokens).Error; err != nil {
		t.Fatalf("create site tokens failed: %v", err)
	}

	models := []model.SiteModel{
		{SiteAccountID: account.ID, GroupKey: model.SiteDefaultGroupKey, ModelName: "gpt-4o-mini", Source: "sync", RouteType: model.SiteModelRouteTypeOpenAIChat, RouteSource: model.SiteModelRouteSourceSyncInferred},
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&models).Error; err != nil {
		t.Fatalf("create site models failed: %v", err)
	}

	return site, account
}

func countChannelBindings(t *testing.T, ctx context.Context, accountID int) int {
	t.Helper()

	var count int64
	if err := dbpkg.GetDB().WithContext(ctx).
		Model(&model.SiteChannelBinding{}).
		Where("site_account_id = ?", accountID).
		Count(&count).Error; err != nil {
		t.Fatalf("count bindings failed: %v", err)
	}
	return int(count)
}

// TestVersionSegmentBackfillProjectsAndMarksComplete 验证：未完成时，回填会
// 重投影存量站点账号（产出补全版本段的 follow_site 渠道），并把完成标记置 true。
func TestVersionSegmentBackfillProjectsAndMarksComplete(t *testing.T) {
	ctx := setupVersionSegmentBackfillTestDB(t)
	_, account := createBackfillFixture(t, ctx)

	done, err := op.SettingGetBool(model.SettingKeyVersionSegmentBackfilled)
	if err != nil {
		t.Fatalf("read flag: %v", err)
	}
	if done {
		t.Fatalf("expected flag to be false before backfill")
	}
	if got := countChannelBindings(t, ctx, account.ID); got != 0 {
		t.Fatalf("expected no bindings before backfill, got %d", got)
	}

	VersionSegmentBackfill(ctx)

	done, err = op.SettingGetBool(model.SettingKeyVersionSegmentBackfilled)
	if err != nil {
		t.Fatalf("read flag after: %v", err)
	}
	if !done {
		t.Fatalf("expected flag to be true after backfill")
	}

	if got := countChannelBindings(t, ctx, account.ID); got != 1 {
		t.Fatalf("expected one projected binding after backfill, got %d", got)
	}

	// follow_site 的 OpenAI Chat 渠道 base_url 应补全默认版本段 /v1。
	var binding model.SiteChannelBinding
	if err := dbpkg.GetDB().WithContext(ctx).
		Where("site_account_id = ?", account.ID).
		First(&binding).Error; err != nil {
		t.Fatalf("load binding failed: %v", err)
	}
	var channel model.Channel
	if err := dbpkg.GetDB().WithContext(ctx).First(&channel, binding.ChannelID).Error; err != nil {
		t.Fatalf("load channel failed: %v", err)
	}
	if len(channel.BaseUrls) != 1 || channel.BaseUrls[0].URL != "https://example.com/v1" {
		t.Fatalf("expected projected channel base URL %q, got %#v", "https://example.com/v1", channel.BaseUrls)
	}
}

// TestVersionSegmentBackfillSkipsWhenAlreadyDone 验证：完成标记已为 true 时，
// 回填直接跳过（不执行重投影，因此不会产出任何渠道）。
func TestVersionSegmentBackfillSkipsWhenAlreadyDone(t *testing.T) {
	ctx := setupVersionSegmentBackfillTestDB(t)
	_, account := createBackfillFixture(t, ctx)

	if err := op.SettingSetString(model.SettingKeyVersionSegmentBackfilled, "true"); err != nil {
		t.Fatalf("seed flag failed: %v", err)
	}

	VersionSegmentBackfill(ctx)

	if got := countChannelBindings(t, ctx, account.ID); got != 0 {
		t.Fatalf("expected backfill to skip (no bindings), got %d", got)
	}
}

// TestVersionSegmentBackfillMarksCompleteWhenNoSites 验证：库中没有任何站点时，
// 回填直接标记完成。
func TestVersionSegmentBackfillMarksCompleteWhenNoSites(t *testing.T) {
	ctx := setupVersionSegmentBackfillTestDB(t)

	VersionSegmentBackfill(ctx)

	done, err := op.SettingGetBool(model.SettingKeyVersionSegmentBackfilled)
	if err != nil {
		t.Fatalf("read flag: %v", err)
	}
	if !done {
		t.Fatalf("expected flag to be true when no sites exist")
	}
}

// TestVersionSegmentBackfillIsIdempotent 验证：重复执行回填不会破坏已投影渠道，
// 且保持完成标记为 true。
func TestVersionSegmentBackfillIsIdempotent(t *testing.T) {
	ctx := setupVersionSegmentBackfillTestDB(t)
	_, account := createBackfillFixture(t, ctx)

	VersionSegmentBackfill(ctx)
	firstRunBindings := countChannelBindings(t, ctx, account.ID)
	firstRunChannelID := loadFirstChannelID(t, ctx, account.ID)

	VersionSegmentBackfill(ctx)

	if got := countChannelBindings(t, ctx, account.ID); got != firstRunBindings {
		t.Fatalf("binding count changed across runs: %d -> %d", firstRunBindings, got)
	}
	if got := loadFirstChannelID(t, ctx, account.ID); got != firstRunChannelID {
		t.Fatalf("projected channel ID changed across runs: %d -> %d", firstRunChannelID, got)
	}

	done, err := op.SettingGetBool(model.SettingKeyVersionSegmentBackfilled)
	if err != nil {
		t.Fatalf("read flag: %v", err)
	}
	if !done {
		t.Fatalf("expected flag to remain true after repeated backfill")
	}
}

func loadFirstChannelID(t *testing.T, ctx context.Context, accountID int) int {
	t.Helper()

	var binding model.SiteChannelBinding
	if err := dbpkg.GetDB().WithContext(ctx).
		Where("site_account_id = ?", accountID).
		Order("id ASC").
		First(&binding).Error; err != nil {
		t.Fatalf("load binding failed: %v", err)
	}
	return binding.ChannelID
}
