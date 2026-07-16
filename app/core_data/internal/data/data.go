package data

import (
	"cwxu-algo/app/common/conf"
	gorm2 "cwxu-algo/app/common/data/gorm"
	redis2 "cwxu-algo/app/common/data/redis"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spidermetrics"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewDataDB, NewDataRDB)

// NewDataDB 从 Data 中提取 DB
func NewDataDB(data *Data) *gorm.DB {
	return data.DB
}

// NewDataRDB 从 Data 中提取 RDB
func NewDataRDB(data *Data) *redis.Client {
	return data.RDB
}

// Data .
type Data struct {
	DB  *gorm.DB
	RDB *redis.Client
}

// NewData .
func NewData(c *conf.Data) (*Data, func(), error) {
	data := &Data{DB: gorm2.InitGorm(c), RDB: redis2.InitRedis(c)}
	migrateModels(data.DB)
	spidermetrics.BindRedis(data.RDB)
	cleanup := func() {
		log.Info("closing the data resources")
		sql, _ := data.DB.DB()
		sql.Close()
	}
	return data, cleanup, nil
}

// migrateModels 合并
func migrateModels(db *gorm.DB) {
	reconcilePlatformDuplicates(db)
	err := db.AutoMigrate(&model.SubmitLog{}, &model.Platform{}, &model.ContestLog{}, &model.Bulletin{}, &model.Problem{}, &model.EmergencyNotice{})
	if err != nil {
		panic("数据库：数据库自动合并失败")
	}
}

// reconcilePlatformDuplicates prepares historical bindings for the new
// (user_id, platform) unique index. Submission and contest rows reference that
// natural key rather than platforms.id, so retaining the newest binding is safe.
func reconcilePlatformDuplicates(db *gorm.DB) {
	if db == nil || !db.Migrator().HasTable(&model.Platform{}) {
		return
	}
	result := db.Exec(`
		DELETE FROM platforms
		WHERE id IN (
			SELECT id FROM (
				SELECT id,
					ROW_NUMBER() OVER (PARTITION BY user_id, platform ORDER BY id DESC) AS duplicate_rank
				FROM platforms
			) AS duplicate_rows
			WHERE duplicate_rank > 1
		)
	`)
	if result.Error != nil {
		panic("数据库：OJ 绑定历史重复数据归并失败")
	}
	if result.RowsAffected > 0 {
		log.Warnf("database migration removed %d duplicate platform bindings", result.RowsAffected)
	}
}
