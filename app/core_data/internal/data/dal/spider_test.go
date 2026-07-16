package dal

import (
	"context"
	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/core_data/internal/data"
	"os"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSpiderDal(t *testing.T) {
	databaseSource := os.Getenv("CWXU_TEST_DATABASE_SOURCE")
	redisAddr := os.Getenv("CWXU_TEST_REDIS_ADDR")
	if databaseSource == "" || redisAddr == "" {
		t.Skip("set CWXU_TEST_DATABASE_SOURCE and CWXU_TEST_REDIS_ADDR to run this integration test")
	}
	c := conf.Data{
		Database: &conf.Data_Database{
			Driver: "postgres",
			Source: databaseSource,
		},
		Redis: &conf.Data_Redis{
			Addr:         redisAddr,
			Password:     os.Getenv("CWXU_TEST_REDIS_PASSWORD"),
			ReadTimeout:  &durationpb.Duration{Nanos: int32(2 * time.Second)},
			WriteTimeout: &durationpb.Duration{Nanos: int32(2 * time.Second)},
		},
	}
	d, _, _ := data.NewData(&c)
	dal := NewSpiderDal(d)
	r, err := dal.GetByUserId(context.Background(), 1, -1, 10)
	if err != nil {
		t.Log(err)
	}
	t.Log(r)
}
