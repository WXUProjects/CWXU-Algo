package dal

import (
	"context"
	"cwxu-algo/app/common/utils"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// SpiderDal 爬虫数据操作模块
type SpiderDal struct {
	db  *gorm.DB
	rdb *redis.Client
}

func NewSpiderDal(data *data.Data) *SpiderDal {
	return &SpiderDal{
		db:  data.DB,
		rdb: data.RDB,
	}
}

// GetByUserId 根据userId获取提交记录
// 设计思路: Redis 查 ID -> Redis 根据ID 查数据 -> 回源DB -> 降级
//
// 参数:
//   - userId 用户ID
//   - lastTimeUnix 上次获取的时间戳
//   - limit 获取数量
func (s *SpiderDal) GetByUserId(ctx context.Context, userId int64, lastTimeUnix int64, limit int64) ([]model.SubmitLog, error) {
	if lastTimeUnix == -1 {
		lastTimeUnix = 33325619029
	}

	cacheKey := fmt.Sprintf("core:submit_log:user:%d", userId)
	res := s.rdb.ZRevRangeByScore(ctx, cacheKey, &redis.ZRangeBy{
		Max:   fmt.Sprintf("(%d", lastTimeUnix),
		Min:   "-inf",
		Count: limit,
	})
	var sbLog []model.SubmitLog
	ids, err := res.Result()
	dbFunc := func() ([]model.SubmitLog, error) {
		// 降级到纯db
		t := time.Unix(lastTimeUnix, 0)
		err := s.db.Order("time DESC").Where("user_id = ? AND time < ?", userId, t).Limit(int(limit)).Find(&sbLog).Error
		go func() {
			ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			s.SetCache(ctx2, sbLog)
		}()
		return sbLog, err
	}
	if err != nil {
		return dbFunc()
	}
	// 防止缓存忽悠人
	t := time.Unix(lastTimeUnix, 0)
	err = s.db.Order("time DESC").Where("user_id = ? AND time < ?", userId, t).Limit(1).Find(&sbLog).Error
	if err != nil || len(ids) < int(limit) || strconv.Itoa(int(sbLog[0].ID)) != ids[0] {
		return dbFunc()
	}
	// 到 Redis 的 Global 查这些ID
	// 构建缓存key
	cacheKeys := make([]string, len(ids))
	for i, id := range ids {
		cacheKeys[i] = fmt.Sprintf("core:submit_log:detail:%s", id)
	}
	r := s.rdb.MGet(ctx, cacheKeys...)
	rVal, err := r.Result()

	// 由于缓存列不存在导致回源
	if err != nil || slices.Contains(rVal, nil) {
		return dbFunc()
	}
	// 命中，解析缓存
	sbLog = make([]model.SubmitLog, 0)
	for _, v := range rVal {
		var l model.SubmitLog
		_ = utils.GobDecoder([]byte(v.(string)), &l)

		sbLog = append(sbLog, l)
	}
	log.Info(sbLog)
	return sbLog, nil
}

// SetCache 缓存提交记录
func (s *SpiderDal) SetCache(ctx context.Context, log []model.SubmitLog) {
	pipe := s.rdb.Pipeline()
	// 根据 userId 构建 Zset
	for _, v := range log {
		cacheKey := fmt.Sprintf("core:submit_log:user:%d", v.UserID)
		_ = pipe.ZAdd(ctx, cacheKey, redis.Z{
			Score:  float64(v.Time.Unix()),
			Member: v.ID,
		})
		// 构建缓存key
		cacheKey = fmt.Sprintf("core:submit_log:detail:%d", v.ID)
		_ = pipe.Expire(ctx, cacheKey, 24*time.Hour)
		// 缓存提交记录
		vByte, _ := utils.GobEncoder(v)
		_ = pipe.Set(ctx, cacheKey, vByte, 12*time.Hour)
	}
	_, _ = pipe.Exec(ctx)
}
