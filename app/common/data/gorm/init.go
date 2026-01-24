package gorm

import (
	"cwxu-algo/app/common/conf"

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
	return db
}
