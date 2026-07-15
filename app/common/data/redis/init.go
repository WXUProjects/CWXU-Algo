package redis

import (
	"cwxu-algo/app/common/conf"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/durationpb"
)

func pbDuration(d *durationpb.Duration, fallback time.Duration) time.Duration {
	if d == nil {
		return fallback
	}
	if v := d.AsDuration(); v > 0 {
		return v
	}
	return fallback
}

func InitRedis(conf *conf.Data) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:         conf.Redis.Addr,
		Password:     conf.Redis.Password,
		DB:           0,
		ReadTimeout:  pbDuration(conf.Redis.ReadTimeout, 2*time.Second),
		WriteTimeout: pbDuration(conf.Redis.WriteTimeout, 2*time.Second),
		DialTimeout:  5 * time.Second,
	})
	return rdb
}
