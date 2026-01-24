package data

import (
	"cwxu-algo/app/common/conf"
	gorm2 "cwxu-algo/app/common/data/gorm"
	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData)

// Data .
type Data struct {
	DB  *gorm.DB
	RDB *redis.Client
}

// NewData .
func NewData(c *conf.Data) (*Data, func(), error) {
	data := &Data{DB: gorm2.InitGorm(c)}
	migrateModels(data.DB)
	cleanup := func() {
		log.Info("closing the data resources")
		sql, _ := data.DB.DB()
		sql.Close()
	}
	return data, cleanup, nil
}

// migrateModels 合并
func migrateModels(db *gorm.DB) {
	err := db.AutoMigrate(&model.SubmitLog{}, &model.Platform{})
	if err != nil {
		panic("数据库：数据库自动合并失败")
	}
}
