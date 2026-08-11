package migrate

import (
	"fmt"

	"github.com/bestruirui/octopus/internal/model"
	"gorm.io/gorm"
)

func init() {
	RegisterAfterAutoMigration(Migration{
		Version: 2026081101,
		Up:      migrateBaseUrlModeAndWSAffinity,
	})
}

func migrateBaseUrlModeAndWSAffinity(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	if db.Migrator().HasTable(&model.Channel{}) {
		if !db.Migrator().HasColumn(&model.Channel{}, "BaseUrlMode") {
			if err := db.Migrator().AddColumn(&model.Channel{}, "BaseUrlMode"); err != nil {
				return err
			}
		}
		if err := db.Model(&model.Channel{}).
			Where("base_url_mode IS NULL OR base_url_mode = 0").
			Update("base_url_mode", int(model.BaseUrlModeDelay)).Error; err != nil {
			return err
		}
	}

	if db.Migrator().HasTable(&model.WSResponseAffinity{}) {
		if !db.Migrator().HasColumn(&model.WSResponseAffinity{}, "BaseURLKey") {
			if err := db.Migrator().AddColumn(&model.WSResponseAffinity{}, "BaseURLKey"); err != nil {
				return err
			}
		}
	}

	return nil
}