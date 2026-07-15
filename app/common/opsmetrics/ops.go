package opsmetrics

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const ttl = 72 * time.Hour
const mauTTL = 40 * 24 * time.Hour

var loc *time.Location

func init() {
	l, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		l = time.FixedZone("CST", 8*3600)
	}
	loc = l
}

func dayKey(t time.Time) string {
	return t.In(loc).Format("20060102")
}

func monthKey(t time.Time) string {
	return t.In(loc).Format("200601")
}

// RecordAPIRequest 记录一次 API 请求，并维护并发峰值
func RecordAPIRequest(ctx context.Context, rdb *redis.Client, service string) func() {
	if rdb == nil {
		return func() {}
	}
	day := dayKey(time.Now())
	reqKey := fmt.Sprintf("ops:api:req:%s", day)
	svcKey := fmt.Sprintf("ops:api:req:%s:%s", day, service)
	inflightKey := "ops:api:inflight"
	peakKey := fmt.Sprintf("ops:api:peak:%s", day)

	pipe := rdb.Pipeline()
	pipe.Incr(ctx, reqKey)
	pipe.Expire(ctx, reqKey, ttl)
	if service != "" {
		pipe.Incr(ctx, svcKey)
		pipe.Expire(ctx, svcKey, ttl)
	}
	pipe.Incr(ctx, inflightKey)
	_, _ = pipe.Exec(ctx)

	// 峰值：当前 inflight 与 peak 取 max
	cur, err := rdb.Get(ctx, inflightKey).Int64()
	if err == nil && cur > 0 {
		// Lua-free：GET peak + SET if higher（竞态可接受，峰值略偏低亦可）
		peak, _ := rdb.Get(ctx, peakKey).Int64()
		if cur > peak {
			_ = rdb.Set(ctx, peakKey, cur, ttl).Err()
		}
	}

	return func() {
		n, err := rdb.Decr(ctx, inflightKey).Result()
		if err == nil && n < 0 {
			_ = rdb.Set(ctx, inflightKey, 0, 0).Err()
		}
	}
}

// IncSpider 爬虫日计数
func IncSpider(ctx context.Context, rdb *redis.Client, kind string, n int64) {
	if rdb == nil || n == 0 {
		return
	}
	day := dayKey(time.Now())
	key := fmt.Sprintf("ops:spider:%s:%s", kind, day)
	pipe := rdb.Pipeline()
	pipe.IncrBy(ctx, key, n)
	pipe.Expire(ctx, key, ttl)
	_, _ = pipe.Exec(ctx)
}

// TouchMAU 登录用户写入月活集合
func TouchMAU(ctx context.Context, rdb *redis.Client, userID uint) {
	if rdb == nil || userID == 0 {
		return
	}
	key := fmt.Sprintf("visit:mau:%s", monthKey(time.Now()))
	pipe := rdb.Pipeline()
	pipe.SAdd(ctx, key, strconv.FormatUint(uint64(userID), 10))
	pipe.Expire(ctx, key, mauTTL)
	_, _ = pipe.Exec(ctx)
}

// Snapshot 读取运维日指标
type Snapshot struct {
	APIRequestsToday int64
	APIPeakToday     int64
	APIInflight      int64
	SpiderEnqueued   int64
	SpiderOK         int64
	SpiderFail       int64
	SpiderRows       int64
	MAU              int64
}

func ReadSnapshot(ctx context.Context, rdb *redis.Client) Snapshot {
	var s Snapshot
	if rdb == nil {
		return s
	}
	day := dayKey(time.Now())
	month := monthKey(time.Now())
	if v, err := rdb.Get(ctx, "ops:api:req:"+day).Int64(); err == nil {
		s.APIRequestsToday = v
	}
	if v, err := rdb.Get(ctx, "ops:api:peak:"+day).Int64(); err == nil {
		s.APIPeakToday = v
	}
	if v, err := rdb.Get(ctx, "ops:api:inflight").Int64(); err == nil && v > 0 {
		s.APIInflight = v
	}
	if v, err := rdb.Get(ctx, "ops:spider:enqueued:"+day).Int64(); err == nil {
		s.SpiderEnqueued = v
	}
	if v, err := rdb.Get(ctx, "ops:spider:ok:"+day).Int64(); err == nil {
		s.SpiderOK = v
	}
	if v, err := rdb.Get(ctx, "ops:spider:fail:"+day).Int64(); err == nil {
		s.SpiderFail = v
	}
	if v, err := rdb.Get(ctx, "ops:spider:rows:"+day).Int64(); err == nil {
		s.SpiderRows = v
	}
	if v, err := rdb.SCard(ctx, "visit:mau:"+month).Result(); err == nil {
		s.MAU = v
	}
	return s
}
