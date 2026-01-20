package gorm

import (
	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/core_data/internal/data/gorm/model"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// InitGorm 初始化GORM 连接数据库
func InitGorm(conf *conf.Data) *gorm.DB {
	var db *gorm.DB
	var err error
	switch conf.Database.Driver {
	case "postgres":
		db, err = gorm.Open(postgres.Open(conf.Database.Source))
		if err != nil {
			panic("数据库：postgres数据库连接失败" + err.Error())
		}
	}
	if db != nil {
		migrateModels(db)
	} else {
		panic("数据库：数据库连接失败")
	}
	return db
}

// migrateModels 合并
func migrateModels(db *gorm.DB) {
	err := db.AutoMigrate(&model.SubmitLog{}, &model.Platform{})
	if err != nil {
		panic("数据库：数据库自动合并失败")
	}
}
