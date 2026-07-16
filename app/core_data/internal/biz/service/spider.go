package service

import (
	"context"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"cwxu-algo/app/core_data/internal/spidermetrics"
	"fmt"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm/clause"
)

type SpiderUseCase struct {
	data    *data.Data
	problem *ProblemUseCase
}

func NewSpiderUseCase(data *data.Data, problem *ProblemUseCase) *SpiderUseCase {
	return &SpiderUseCase{
		data:    data,
		problem: problem,
	}
}

// loadDataTimeout 单用户整次爬取上限，防止某平台挂死占满 worker 导致 spider 队列堆积
const loadDataTimeout = 8 * time.Minute

// LoadData 加载数据。platform 非空时只抓该平台；空则抓全部已绑定平台。
// 无绑定平台时成功返回；有平台且全部失败则返回 error（consumer 可重试）。
func (uc *SpiderUseCase) LoadData(userId int64, needAll bool, platform string) error {
	// 无论如何，函数退出前一定删缓存
	defer uc.invalidateCache(userId)

	ctx, cancel := context.WithTimeout(context.Background(), loadDataTimeout)
	defer cancel()

	var platforms []model.Platform
	q := uc.data.DB.Where("user_id = ?", userId)
	if platform != "" {
		q = q.Where("platform = ?", platform)
	}
	if err := q.Find(&platforms).Error; err != nil {
		return err
	}
	if len(platforms) == 0 {
		return nil
	}

	var failCount int
	var lastErr error
	for _, plat := range platforms {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("load data timeout user=%d after partial: %w", userId, err)
		}
		if err := uc.loadOnePlatform(userId, plat, needAll); err != nil {
			failCount++
			lastErr = err
		}
	}
	if failCount == len(platforms) && lastErr != nil {
		return fmt.Errorf("all platforms failed for user %d: %w", userId, lastErr)
	}
	return nil
}
func (uc *SpiderUseCase) fetchAndSave(userId int64, plat model.Platform, needAll bool) error {
	p, ok := spider.Get(plat.Platform)
	if !ok {
		return fmt.Errorf("平台插件不存在")
	}
	sbFetch, ok := p.(spider.SubmitLogFetcher)
	if !ok {
		return fmt.Errorf("平台未实现 SubmitLogFetcher")
	}
	tmp, err := sbFetch.FetchSubmitLog(userId, plat.Username, needAll)
	if err != nil {
		return err
	}
	if len(tmp) == 0 {
		return nil
	}

	res := uc.data.DB.
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "submit_id"}},
			DoNothing: true,
		}).
		Create(&tmp)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected > 0 {
		spidermetrics.IncRows(res.RowsAffected)
	}
	return nil
}

func (uc *SpiderUseCase) fetchAndSaveContest(userId int64, plat model.Platform, needAll bool) error {
	p, ok := spider.Get(plat.Platform)
	if !ok {
		return fmt.Errorf("平台插件不存在")
	}
	sbFetch, ok := p.(spider.SubmitContestFetcher)
	if !ok {
		return fmt.Errorf("平台未实现 SubmitContestFetcher")
	}
	tmp, err := sbFetch.FetchContestLog(userId, plat.Username, needAll)
	if err != nil {
		return err
	}
	if len(tmp) == 0 {
		return nil
	}

	return uc.data.DB.
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "contest_id"}, {Name: "user_id"}},
			DoNothing: true,
		}).
		Save(&tmp).Error
}

func (uc *SpiderUseCase) loadOnePlatform(userId int64, plat model.Platform, needAll bool) error {
	// needAll 全量：最多 3 次（原先 12 次会把 worker 占死、队列堆积）
	maxRetries := 1
	if needAll {
		maxRetries = 3
	}
	for i := 0; i < maxRetries; i++ {
		err := uc.fetchAndSave(userId, plat, needAll)
		if contestErr := uc.fetchAndSaveContest(userId, plat, needAll); contestErr != nil {
			log.Errorf("Spider: fetchAndSaveContest %s %s 失败: %v", plat.Platform, plat.Username, contestErr)
		}
		if err == nil {
			log.Infof("Spider: %s %s 成功", plat.Platform, plat.Username)
			uc.invalidateCache(userId)
			if uc.problem != nil {
				uc.problem.BindSubmitsAfterSpider(userId)
			}
			return nil
		}
		if strings.Contains(err.Error(), "平台") {
			log.Errorf(
				"Spider: %s %s 失败: %v",
				plat.Platform,
				plat.Username,
				err,
			)
			return err
		}
		log.Errorf(
			"Spider: %s %s 失败 (重试 %d/%d): %v",
			plat.Platform,
			plat.Username,
			i+1,
			maxRetries,
			err,
		)
		if !needAll || i+1 >= maxRetries {
			return err
		}
		time.Sleep(3 * time.Second)
	}
	return fmt.Errorf("platform %s max retries exceeded", plat.Platform)
}
func (uc *SpiderUseCase) invalidateCache(userId int64) {
	ctx := context.Background()
	rdb := uc.data.RDB

	// 1. 精确 key，直接删
	_ = rdb.Del(
		ctx,
		fmt.Sprintf("core:submit_log:user:%d", userId),
		fmt.Sprintf("user:%d:lastSubmitTime", userId),
		fmt.Sprintf("core:contest_log:user:%d", userId),
	).Err()

	// 2. period / heatmap 用全局版本号失效（含组织 statistic:period:org:{id}:v*）
	// 旧无版本 key 也会被 SCAN 清掉，避免脏缓存卡 48h 导致「组织统计像个人」。
	_ = rdb.Incr(ctx, "statistic:heatmap:global:ver").Err()
	_ = rdb.Incr(ctx, "statistic:period:global:ver").Err()

	patterns := []string{
		fmt.Sprintf("statistic:heatmap:%d:*:*:*", userId),
		"statistic:period:*",
		fmt.Sprintf("core:contest_log:user:%d:*", userId),
	}

	for _, pattern := range patterns {
		var cursor uint64
		for {
			keys, next, err := rdb.Scan(ctx, cursor, pattern, 100).Result()
			if err != nil {
				log.Errorf("scan pattern %s failed: %v", pattern, err)
				break
			}
			if len(keys) > 0 {
				_ = rdb.Unlink(ctx, keys...).Err()
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
}
