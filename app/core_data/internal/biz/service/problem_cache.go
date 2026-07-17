package service

import (
	"context"
	"fmt"
	"time"

	data2 "cwxu-algo/app/common/data"
	"cwxu-algo/app/common/utils"
	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
)

const problemDetailCacheSchema = "1"

func problemDetailVerKey(id uint) string {
	return fmt.Sprintf("problem:detail:ver:%d", id)
}

func problemDetailCacheKey(id uint, ver string) string {
	if ver == "" {
		ver = "0"
	}
	return fmt.Sprintf("problem:detail:s%s:%d:v%s", problemDetailCacheSchema, id, ver)
}

// BumpProblemDetailVer 题面/标签变更后失效详情缓存
func (uc *ProblemUseCase) BumpProblemDetailVer(id uint) {
	if uc.data == nil || uc.data.RDB == nil || id == 0 {
		return
	}
	_ = uc.data.RDB.Incr(context.Background(), problemDetailVerKey(id)).Err()
}

func (uc *ProblemUseCase) getProblemCached(id uint) (*model.Problem, error) {
	if id == 0 {
		return nil, fmt.Errorf("invalid id")
	}
	ctx := context.Background()
	if uc.data.RDB != nil {
		ver := "0"
		if v, err := uc.data.RDB.Get(ctx, problemDetailVerKey(id)).Result(); err == nil && v != "" {
			ver = v
		}
		key := problemDetailCacheKey(id, ver)
		if b, err := uc.data.RDB.Get(ctx, key).Bytes(); err == nil && len(b) > 0 {
			var p model.Problem
			if e := utils.GobDecoder(b, &p); e == nil && p.ID != 0 {
				return &p, nil
			}
		}
		// miss：回源 + 写缓存
		p, err := uc.loadProblemDB(id)
		if err != nil {
			return nil, err
		}
		if b, e := utils.GobEncoder(p); e == nil {
			_ = uc.data.RDB.Set(ctx, key, b, data2.DefaultCacheTTL).Err()
		}
		return p, nil
	}
	return uc.loadProblemDB(id)
}

func (uc *ProblemUseCase) loadProblemDB(id uint) (*model.Problem, error) {
	var p model.Problem
	if err := uc.data.DB.First(&p, id).Error; err != nil {
		return nil, err
	}
	return &p, nil
}

// --- Progress status counters (Redis Hash) ---

const progressStatusHashKey = "problem:progress:status"

func (uc *ProblemUseCase) progressMoveStatus(from, to string) {
	if uc.data == nil || uc.data.RDB == nil || to == "" {
		return
	}
	ctx := context.Background()
	pipe := uc.data.RDB.Pipeline()
	if from != "" && from != to {
		pipe.HIncrBy(ctx, progressStatusHashKey, from, -1)
	}
	pipe.HIncrBy(ctx, progressStatusHashKey, to, 1)
	if _, err := pipe.Exec(ctx); err != nil {
		log.Debugf("progressMoveStatus %s→%s: %v", from, to, err)
	}
}

func (uc *ProblemUseCase) progressCountersFromRedis() (map[string]int64, bool) {
	if uc.data == nil || uc.data.RDB == nil {
		return nil, false
	}
	m, err := uc.data.RDB.HGetAll(context.Background(), progressStatusHashKey).Result()
	if err != nil || len(m) == 0 {
		return nil, false
	}
	out := map[string]int64{}
	var sum int64
	for k, v := range m {
		var n int64
		fmt.Sscanf(v, "%d", &n)
		if n < 0 {
			n = 0
		}
		out[k] = n
		sum += n
	}
	if sum == 0 {
		return nil, false
	}
	return out, true
}

func (uc *ProblemUseCase) rebuildProgressCounters() map[string]int64 {
	type sc struct {
		Status string
		Count  int64
	}
	var rows []sc
	_ = uc.data.DB.Model(&model.Problem{}).
		Select("status, COUNT(*) as count").
		Group("status").
		Scan(&rows).Error
	out := map[string]int64{}
	for _, r := range rows {
		out[r.Status] = r.Count
	}
	if uc.data.RDB != nil {
		ctx := context.Background()
		_ = uc.data.RDB.Del(ctx, progressStatusHashKey).Err()
		if len(out) > 0 {
			fields := map[string]interface{}{}
			for k, v := range out {
				fields[k] = v
			}
			_ = uc.data.RDB.HSet(ctx, progressStatusHashKey, fields).Err()
			_ = uc.data.RDB.Expire(ctx, progressStatusHashKey, 7*24*time.Hour).Err()
		}
	}
	return out
}
