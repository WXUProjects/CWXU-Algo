package data

import (
	"cwxu-algo/app/user/internal/conf"
	gorm2 "cwxu-algo/app/user/internal/data/gorm"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData)

// Data .
type Data struct {
	DB *gorm.DB
}

// NewData .
func NewData(c *conf.Data) (*Data, func(), error) {
	data := &Data{DB: gorm2.InitGorm(c)}
	cleanup := func() {
		log.Info("closing the data resources")
		sql, _ := data.DB.DB()
		sql.Close()
	}
	return data, cleanup, nil
}
