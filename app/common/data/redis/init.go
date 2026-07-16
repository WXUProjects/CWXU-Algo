package redis

import (
	"cwxu-algo/app/common/conf"
	"os"
	"strconv"
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

// InitRedis 2c4g 单机：每进程小池，多服务合计连接可控。
// 覆盖：CWXU_REDIS_POOL_SIZE / CWXU_REDIS_MIN_IDLE
func InitRedis(conf *conf.Data) *redis.Client {
	poolSize := envInt("CWXU_REDIS_POOL_SIZE", 10)
	minIdle := envInt("CWXU_REDIS_MIN_IDLE", 2)
	if minIdle > poolSize {
		minIdle = poolSize
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:            conf.Redis.Addr,
		Password:        conf.Redis.Password,
		DB:              0,
		ReadTimeout:     pbDuration(conf.Redis.ReadTimeout, 2*time.Second),
		WriteTimeout:    pbDuration(conf.Redis.WriteTimeout, 2*time.Second),
		DialTimeout:     5 * time.Second,
		PoolSize:        poolSize,
		MinIdleConns:    minIdle,
		PoolTimeout:     3 * time.Second,
		ConnMaxIdleTime: 5 * time.Minute,
		ConnMaxLifetime:  20 * time.Minute,
	})
	return rdb
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
