package migrate

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func newSiteEndpointMigrationDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.Exec(`CREATE TABLE sites (id integer primary key, route_base_urls text)`).Error; err != nil {
		t.Fatalf("create legacy sites table: %v", err)
	}
	return db
}

func loadMigratedEndpointConfig(t *testing.T, db *gorm.DB, id int) model.SiteModelEndpointConfig {
	t.Helper()
	var raw string
	if err := db.Table("sites").Where("id = ?", id).Pluck("model_endpoint_config", &raw).Error; err != nil {
		t.Fatalf("load model_endpoint_config: %v", err)
	}
	var config model.SiteModelEndpointConfig
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		t.Fatalf("decode model_endpoint_config %q: %v", raw, err)
	}
	return config
}

func TestMigrateSiteModelEndpointConfigFromLegacy(t *testing.T) {
	db := newSiteEndpointMigrationDB(t)
	legacy := `[{"route_type":"anthropic","base_url":"https://api.example/anthropic///?signature=a/+%2F"},{"route_type":"gemini","base_url":"https://api.example/gemini"}]`
	if err := db.Exec("INSERT INTO sites (id, route_base_urls) VALUES (?, ?), (?, NULL)", 1, legacy, 2).Error; err != nil {
		t.Fatalf("insert legacy sites: %v", err)
	}

	if err := migrateSiteModelEndpointConfig(db); err != nil {
		t.Fatalf("migrateSiteModelEndpointConfig: %v", err)
	}
	if !db.Migrator().HasColumn(&model.Site{}, "ModelEndpointConfig") {
		t.Fatal("model_endpoint_config column was not created")
	}

	config := loadMigratedEndpointConfig(t, db, 1)
	if config.Default.Source != model.SiteModelEndpointSourceFollowSite || len(config.RouteOverrides) != 2 {
		t.Fatalf("migrated config = %#v", config)
	}
	gotURL := config.RouteOverrides[0].EndpointSet.BaseURLs[0].URL
	wantURL := "https://api.example/anthropic?signature=a/+%2F"
	if gotURL != wantURL {
		t.Fatalf("migrated url = %q, want %q", gotURL, wantURL)
	}
	if got := config.RouteOverrides[1]; got.RouteType != model.SiteModelRouteTypeGemini || got.EndpointSet.BaseURLs[0].URL != "https://api.example/gemini" {
		t.Fatalf("second migrated override = %#v", got)
	}

	empty := loadMigratedEndpointConfig(t, db, 2)
	if empty.Default.Source != model.SiteModelEndpointSourceFollowSite || len(empty.RouteOverrides) != 0 {
		t.Fatalf("empty legacy config = %#v", empty)
	}
}

func TestMigrateSiteModelEndpointConfigIsIdempotent(t *testing.T) {
	db := newSiteEndpointMigrationDB(t)
	if err := db.Exec("INSERT INTO sites (id, route_base_urls) VALUES (?, ?)", 1, `[]`).Error; err != nil {
		t.Fatalf("insert legacy site: %v", err)
	}
	if err := migrateSiteModelEndpointConfig(db); err != nil {
		t.Fatalf("first migration: %v", err)
	}

	custom := model.SiteModelEndpointConfig{Default: model.SiteModelEndpointDefault{
		Source: model.SiteModelEndpointSourceCustom,
		EndpointSet: &model.SiteEndpointSet{
			BaseURLMode: model.BaseUrlModeFailover,
			BaseURLs:    []model.SiteModelEndpoint{{URL: "https://custom.example/v1"}},
		},
	}}
	encoded, err := json.Marshal(custom)
	if err != nil {
		t.Fatalf("encode custom config: %v", err)
	}
	if err := db.Table("sites").Where("id = ?", 1).Update("model_endpoint_config", string(encoded)).Error; err != nil {
		t.Fatalf("seed custom config: %v", err)
	}
	if err := migrateSiteModelEndpointConfig(db); err != nil {
		t.Fatalf("second migration: %v", err)
	}
	got := loadMigratedEndpointConfig(t, db, 1)
	if got.Default.Source != model.SiteModelEndpointSourceCustom || got.Default.EndpointSet.BaseURLs[0].URL != "https://custom.example/v1" {
		t.Fatalf("idempotent config = %#v", got)
	}
}

func TestMigrateSiteModelEndpointConfigRejectsMalformedLegacyJSON(t *testing.T) {
	db := newSiteEndpointMigrationDB(t)
	if err := db.Exec("INSERT INTO sites (id, route_base_urls) VALUES (?, ?)", 7, `{broken`).Error; err != nil {
		t.Fatalf("insert malformed site: %v", err)
	}
	err := migrateSiteModelEndpointConfig(db)
	if err == nil || !strings.Contains(err.Error(), "site 7 route_base_urls is invalid") {
		t.Fatalf("migration error = %v", err)
	}
}
