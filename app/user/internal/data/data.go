package data

import (
	"cwxu-algo/app/common/conf"
	gorm2 "cwxu-algo/app/common/data/gorm"
	redis2 "cwxu-algo/app/common/data/redis"
	"cwxu-algo/app/user/internal/data/model"

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
	data := &Data{DB: gorm2.InitGorm(c), RDB: redis2.InitRedis(c)}
	migrateModels(data.DB)
	cleanup := func() {
		log.Info("closing the data resources")
		sql, _ := data.DB.DB()
		sql.Close()
		data.RDB.Close()
	}
	return data, cleanup, nil
}

// migrateModels 合并
func migrateModels(db *gorm.DB) {
	err := db.AutoMigrate(
		&model.User{},
		&model.Group{},
	)
	if err != nil {
		panic("数据库：数据库自动合并失败")
	}
}
