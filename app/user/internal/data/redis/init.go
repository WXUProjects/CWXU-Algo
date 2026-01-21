package redis

import (
	"cwxu-algo/app/common/conf"
	"time"

	"github.com/redis/go-redis/v9"
)

func InitRedis(conf *conf.Data) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:         conf.Redis.Addr,
		Password:     conf.Redis.Password,
		DB:           0,
		ReadTimeout:  time.Duration(conf.Redis.ReadTimeout.Nanos),
		WriteTimeout: time.Duration(conf.Redis.WriteTimeout.Nanos),
	})
	return rdb
}
