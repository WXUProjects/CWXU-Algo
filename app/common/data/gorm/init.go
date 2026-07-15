package gorm

import (
	"cwxu-algo/app/common/conf"
	"time"

	"github.com/go-kratos/kratos/v2/log"
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
	if db == nil {
		panic("数据库：数据库连接失败")
	}
	sqlDB, err := db.DB()
	if err != nil {
		panic("数据库：获取连接池失败" + err.Error())
	}
	sqlDB.SetMaxOpenConns(40)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	sqlDB.SetConnMaxIdleTime(10 * time.Minute)
	log.Info("数据库：连接池已配置 MaxOpen=40 MaxIdle=10")
	return db
}
