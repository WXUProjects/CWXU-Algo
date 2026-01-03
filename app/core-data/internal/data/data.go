package data

import (
	"cwxu-algo/app/core-data/internal/conf"
	gorm2 "cwxu-algo/app/core-data/internal/data/gorm"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData)

// Data .
type Data struct {
	db *gorm.DB
}

// NewData .
func NewData(c *conf.Data) (*Data, func(), error) {
	data := &Data{db: gorm2.InitGorm(c)}
	cleanup := func() {
		log.Info("closing the data resources")
		sql, _ := data.db.DB()
		sql.Close()
	}
	return data, cleanup, nil
}
