package dal

import (
	"context"
	"cwxu-algo/app/common/utils"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"
	"fmt"
	"slices"
	"time"

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
	return s.GetByUserIdScoped(ctx, userId, lastTimeUnix, limit, nil)
}

// GetByUserIdScoped userId=-1 时用 memberIDs 限制组织；memberIDs 为空切片表示无数据；nil 且 userId=-1 表示旧全站（应避免）
func (s *SpiderDal) GetByUserIdScoped(ctx context.Context, userId int64, lastTimeUnix int64, limit int64, memberIDs []int64) ([]model.SubmitLog, error) {
	if lastTimeUnix == -1 {
		lastTimeUnix = 33325619029
	}

	// 组织聚合流不做 redis 缓存（成员集合会变）
	if userId == -1 {
		t := time.Unix(lastTimeUnix, 0)
		var sbLog []model.SubmitLog
		q := s.db.WithContext(ctx).Order("time DESC").Where("time < ?", t)
		if memberIDs != nil {
			if len(memberIDs) == 0 {
				return []model.SubmitLog{}, nil
			}
			q = q.Where("user_id IN ?", memberIDs)
		}
		err := q.Limit(int(limit)).Find(&sbLog).Error
		return sbLog, err
	}

	cacheKey := fmt.Sprintf("core:submit_log:user:%d", userId)
	res := s.rdb.ZRevRangeByScore(ctx, cacheKey, &redis.ZRangeBy{
		Max:   fmt.Sprintf("(%d", lastTimeUnix),
		Min:   "-inf",
		Count: limit,
	})
	var sbLog []model.SubmitLog
	ids, err := res.Result()
	t := time.Unix(lastTimeUnix, 0)
	q := s.db.WithContext(ctx).Order("time DESC").Where("user_id = ? AND time < ?", userId, t)
	dbFunc := func() ([]model.SubmitLog, error) {
		err := q.Limit(int(limit)).Find(&sbLog).Error
		if err == nil {
			s.SetCache(ctx, sbLog, userId)
		}
		return sbLog, err
	}
	if err != nil {
		return dbFunc()
	}
	if len(ids) == 0 {
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
		s, ok := v.(string)
		if !ok {
			return dbFunc()
		}
		if err := utils.GobDecoder([]byte(s), &l); err != nil {
			return dbFunc()
		}

		sbLog = append(sbLog, l)
	}
	return sbLog, nil
}

// submitLogCacheKeep 每用户 ZSET 最多保留条数（列表接口分页用，无需缓存全量 10w+）
const submitLogCacheKeep = 300

// SetCache 缓存提交记录（ZSET 裁剪，避免随提交量无界膨胀）
func (s *SpiderDal) SetCache(ctx context.Context, log []model.SubmitLog, userId int64) {
	pipe := s.rdb.Pipeline()
	userKey := fmt.Sprintf("core:submit_log:user:%d", userId)
	// 根据 userId 构建 Zset
	for _, v := range log {
		_ = pipe.ZAdd(ctx, userKey, redis.Z{
			Score:  float64(v.Time.Unix()),
			Member: v.ID,
		})
		// 构建缓存key
		cacheKey := fmt.Sprintf("core:submit_log:detail:%d", v.ID)
		// 缓存提交记录
		vByte, _ := utils.GobEncoder(v)
		_ = pipe.Set(ctx, cacheKey, vByte, 12*time.Hour)
	}
	// 只保留 score 最高（最新）的 N 条，防止日增 2w 把 ZSET 撑爆
	_ = pipe.ZRemRangeByRank(ctx, userKey, 0, int64(-submitLogCacheKeep-1))
	_ = pipe.Expire(ctx, userKey, 12*time.Hour)
	_, _ = pipe.Exec(ctx)
}

// GetContestByUserId 获取用户比赛历史
func (s *SpiderDal) GetContestByUserId(ctx context.Context, userId int64, cursor int64, limit int64, platform string) ([]model.ContestLog, error) {
	if cursor == 0 {
		cursor = 33325619029
	}

	ver := "0"
	if v, err := s.rdb.Get(ctx, fmt.Sprintf("core:contest_log:user:%d:ver", userId)).Result(); err == nil {
		ver = v
	}
	cacheKey := fmt.Sprintf("core:contest_log:user:%d:v%s", userId, ver)
	if platform != "" {
		cacheKey = fmt.Sprintf("core:contest_log:user:%d:%s:v%s", userId, platform, ver)
	}

	res := s.rdb.ZRevRangeByScore(ctx, cacheKey, &redis.ZRangeBy{
		Max:   fmt.Sprintf("(%d", cursor),
		Min:   "-inf",
		Count: limit,
	})
	var contestLogs []model.ContestLog
	ids, err := res.Result()
	t := time.Unix(cursor, 0)

	q := s.db.WithContext(ctx).Order("time DESC")
	if userId != -1 {
		q = q.Where("user_id = ? AND time < ?", userId, t)
	} else {
		q = q.Where("time < ?", t)
	}
	if platform != "" {
		q = q.Where("platform = ?", platform)
	}

	dbFunc := func() ([]model.ContestLog, error) {
		err := q.Limit(int(limit)).Find(&contestLogs).Error
		if err == nil {
			s.SetContestCache(ctx, contestLogs, userId, platform)
		}
		return contestLogs, err
	}

	if err != nil {
		return dbFunc()
	}

	if len(ids) == 0 {
		return dbFunc()
	}

	cacheKeys := make([]string, len(ids))
	for i, id := range ids {
		cacheKeys[i] = fmt.Sprintf("core:contest_log:detail:%s", id)
	}
	r := s.rdb.MGet(ctx, cacheKeys...)
	rVal, err := r.Result()

	if err != nil || slices.Contains(rVal, nil) {
		return dbFunc()
	}

	contestLogs = make([]model.ContestLog, 0)
	for _, v := range rVal {
		var l model.ContestLog
		s, ok := v.(string)
		if !ok {
			return dbFunc()
		}
		if err := utils.GobDecoder([]byte(s), &l); err != nil {
			return dbFunc()
		}
		contestLogs = append(contestLogs, l)
	}
	return contestLogs, nil
}

// GetContestList 获取比赛列表（按 contest_id 去重）
func (s *SpiderDal) GetContestList(ctx context.Context, userId int64, offset int64, limit int64, platform string) ([]model.ContestLog, int64, error) {
	return s.GetContestListScoped(ctx, userId, offset, limit, platform, nil)
}

// GetContestListScoped userId=-1 时 memberIDs 限制组织成员
func (s *SpiderDal) GetContestListScoped(ctx context.Context, userId int64, offset int64, limit int64, platform string, memberIDs []int64) ([]model.ContestLog, int64, error) {
	if memberIDs != nil && len(memberIDs) == 0 && userId == -1 {
		return []model.ContestLog{}, 0, nil
	}
	buildQuery := func() *gorm.DB {
		q := s.db.WithContext(ctx).Model(&model.ContestLog{})
		if userId != -1 {
			q = q.Where("user_id = ?", userId)
		} else if memberIDs != nil {
			q = q.Where("user_id IN ?", memberIDs)
		}
		if platform != "" {
			q = q.Where("platform = ?", platform)
		}
		return q
	}

	// 1. 计算去重后的总数
	var total int64
	countQuery := buildQuery().Select("COUNT(DISTINCT contest_id)")
	if err := countQuery.Scan(&total).Error; err != nil {
		return nil, 0, err
	}

	var contestLogs []model.ContestLog
	ranked := buildQuery().Select("contest_logs.*, ROW_NUMBER() OVER (PARTITION BY contest_id ORDER BY time DESC, id DESC) AS row_num")
	if err := s.db.WithContext(ctx).Table("(?) AS ranked", ranked).
		Where("row_num = 1").Order("time DESC, id DESC").
		Offset(int(offset)).Limit(int(limit)).Scan(&contestLogs).Error; err != nil {
		return nil, 0, err
	}
	return contestLogs, total, nil
}

// GetContestRanking 获取比赛排行榜
func (s *SpiderDal) GetContestRanking(ctx context.Context, contestId string, platform string, offset int64, limit int64, userIds []int64) ([]model.ContestLog, int64, error) {
	var contestLogs []model.ContestLog
	var total int64

	if userIds != nil && len(userIds) == 0 {
		return []model.ContestLog{}, 0, nil
	}
	q := s.db.WithContext(ctx).Model(&model.ContestLog{}).Where("contest_id = ? and platform = ?", contestId, platform)

	if len(userIds) > 0 {
		q = q.Where("user_id IN ?", userIds)
	}

	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if err := q.Order("rank ASC").Offset(int(offset)).Limit(int(limit)).Find(&contestLogs).Error; err != nil {
		return nil, 0, err
	}

	return contestLogs, total, nil
}

// contestLogCacheKeep 每用户比赛 ZSET 上限
const contestLogCacheKeep = 200

// SetContestCache 缓存比赛记录
func (s *SpiderDal) SetContestCache(ctx context.Context, logs []model.ContestLog, userId int64, platform string) {
	pipe := s.rdb.Pipeline()

	ver := "0"
	if v, err := s.rdb.Get(ctx, fmt.Sprintf("core:contest_log:user:%d:ver", userId)).Result(); err == nil {
		ver = v
	}
	cacheKey := fmt.Sprintf("core:contest_log:user:%d:v%s", userId, ver)
	if platform != "" {
		cacheKey = fmt.Sprintf("core:contest_log:user:%d:%s:v%s", userId, platform, ver)
	}

	for _, v := range logs {
		_ = pipe.ZAdd(ctx, cacheKey, redis.Z{
			Score:  float64(v.Time.Unix()),
			Member: v.ID,
		})
		detailKey := fmt.Sprintf("core:contest_log:detail:%d", v.ID)
		vByte, _ := utils.GobEncoder(v)
		_ = pipe.Set(ctx, detailKey, vByte, 12*time.Hour)
	}
	_ = pipe.ZRemRangeByRank(ctx, cacheKey, 0, int64(-contestLogCacheKeep-1))
	_ = pipe.Expire(ctx, cacheKey, 12*time.Hour)
	_, _ = pipe.Exec(ctx)
}
