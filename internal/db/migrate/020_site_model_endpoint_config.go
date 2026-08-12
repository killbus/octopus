package migrate

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bestruirui/octopus/internal/model"
	"gorm.io/gorm"
)

func init() {
	RegisterAfterAutoMigration(Migration{
		Version: 2026081201,
		Up:      migrateSiteModelEndpointConfig,
	})
}

func migrateSiteModelEndpointConfig(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if !db.Migrator().HasTable(&model.Site{}) {
		return nil
	}
	if !db.Migrator().HasColumn(&model.Site{}, "ModelEndpointConfig") {
		if err := db.Migrator().AddColumn(&model.Site{}, "ModelEndpointConfig"); err != nil {
			return err
		}
	}

	hasLegacyColumn := db.Migrator().HasColumn(&model.Site{}, "route_base_urls")
	selectColumns := "id, model_endpoint_config"
	if hasLegacyColumn {
		selectColumns += ", route_base_urls"
	}
	rows, err := db.Table("sites").Select(selectColumns).Rows()
	if err != nil {
		return err
	}
	defer rows.Close()

	type siteEndpointMigrationRow struct {
		id      int
		current sql.NullString
		legacy  sql.NullString
	}
	pending := make([]siteEndpointMigrationRow, 0)
	for rows.Next() {
		var row siteEndpointMigrationRow
		if hasLegacyColumn {
			err = rows.Scan(&row.id, &row.current, &row.legacy)
		} else {
			err = rows.Scan(&row.id, &row.current)
		}
		if err != nil {
			return err
		}
		pending = append(pending, row)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, row := range pending {
		if current := strings.TrimSpace(row.current.String); row.current.Valid && current != "" && current != "null" && current != "{}" {
			var config model.SiteModelEndpointConfig
			if err := json.Unmarshal([]byte(current), &config); err != nil {
				return fmt.Errorf("site %d model_endpoint_config is invalid: %w", row.id, err)
			}
			config = model.NormalizeSiteModelEndpointConfig(config)
			if err := model.ValidateSiteModelEndpointConfig(config); err != nil {
				return fmt.Errorf("site %d model_endpoint_config is invalid: %w", row.id, err)
			}
			continue
		}

		legacy := make([]model.SiteRouteBaseURL, 0)
		if raw := strings.TrimSpace(row.legacy.String); row.legacy.Valid && raw != "" && raw != "null" && raw != "[]" {
			if err := json.Unmarshal([]byte(raw), &legacy); err != nil {
				return fmt.Errorf("site %d route_base_urls is invalid: %w", row.id, err)
			}
		}
		config := model.NormalizeSiteModelEndpointConfig(model.SiteModelEndpointConfigFromLegacy(legacy))
		if err := model.ValidateSiteModelEndpointConfig(config); err != nil {
			return fmt.Errorf("site %d route_base_urls is invalid: %w", row.id, err)
		}
		encoded, err := json.Marshal(config)
		if err != nil {
			return fmt.Errorf("site %d model endpoint config encode failed: %w", row.id, err)
		}
		if err := db.Table("sites").Where("id = ?", row.id).Update("model_endpoint_config", string(encoded)).Error; err != nil {
			return fmt.Errorf("site %d model endpoint config update failed: %w", row.id, err)
		}
	}
	return nil
}
