package dal

import (
	"context"
	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/core_data/internal/data"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSpiderDal(t *testing.T) {
	c := conf.Data{
		Database: &conf.Data_Database{
			Driver: "postgres",
			Source: "host=192.168.1.7 user=cwxu password=cwxu dbname=algo_core_data port=5432 sslmode=disable TimeZone=Asia/Shanghai",
		},
		Redis: &conf.Data_Redis{
			Addr:         "192.168.1.7:6379",
			Password:     "cwxu",
			ReadTimeout:  &durationpb.Duration{Nanos: int32(2 * time.Second)},
			WriteTimeout: &durationpb.Duration{Nanos: int32(2 * time.Second)},
		},
	}
	d, _, _ := data.NewData(&c)
	dal := NewSpiderDal(d)
	r := dal.GetByUserId(context.Background(), 1, -1, 10)
	t.Log(r)
}
