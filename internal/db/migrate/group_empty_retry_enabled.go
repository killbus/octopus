package migrate

import (
	"fmt"

	"github.com/bestruirui/octopus/internal/model"
	"gorm.io/gorm"
)

func init() {
	RegisterAfterAutoMigration(Migration{
		Version: 2026091301,
		Up:      migrateGroupEmptyRetryEnabled,
	})
}

// migrateGroupEmptyRetryEnabled 为 groups 表补充 empty_retry_enabled 列
// （空输出保持与重试开关，default false）。AutoMigrate 已覆盖新库；
// 旧库经 HasColumn/AddColumn 守卫补列，行为回滚 = 关闭开关，无需迁移回滚。
func migrateGroupEmptyRetryEnabled(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	if db.Migrator().HasTable(&model.Group{}) {
		if !db.Migrator().HasColumn(&model.Group{}, "EmptyRetryEnabled") {
			if err := db.Migrator().AddColumn(&model.Group{}, "EmptyRetryEnabled"); err != nil {
				return err
			}
		}
	}

	return nil
}
