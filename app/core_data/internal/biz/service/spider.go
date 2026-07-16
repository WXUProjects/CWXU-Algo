package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/dal"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"cwxu-algo/app/core_data/internal/spidermetrics"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm/clause"
)

const (
	// submitInsertBatchSize 批量 upsert；2c4g 上 300 比 500 更平滑
	submitInsertBatchSize = 300
	// globalCacheBumpMinInterval 1w 日活：组织 ver 更长节流，避免统计 thrash
	globalCacheBumpMinInterval = 2 * time.Minute
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
// 仅在有新写入时失效缓存，避免空跑爬虫打穿 period/heatmap 缓存。
func (uc *SpiderUseCase) LoadData(userId int64, needAll bool, platform string) error {
	var dirty bool
	defer func() {
		if dirty {
			uc.invalidateCache(userId)
		}
	}()

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
		changed, err := uc.loadOnePlatform(userId, plat, needAll)
		if changed {
			dirty = true
		}
		if err != nil {
			failCount++
			lastErr = err
		}
	}
	if failCount == len(platforms) && lastErr != nil {
		return fmt.Errorf("all platforms failed for user %d: %w", userId, lastErr)
	}
	return nil
}

// fetchAndSave 拉取并写入提交；返回新插入行数
func (uc *SpiderUseCase) fetchAndSave(userId int64, plat model.Platform, needAll bool) (int64, error) {
	p, ok := spider.Get(plat.Platform)
	if !ok {
		return 0, fmt.Errorf("平台插件不存在")
	}
	sbFetch, ok := p.(spider.SubmitLogFetcher)
	if !ok {
		return 0, fmt.Errorf("平台未实现 SubmitLogFetcher")
	}
	tmp, err := sbFetch.FetchSubmitLog(userId, plat.Username, needAll)
	if err != nil {
		return 0, err
	}
	if len(tmp) == 0 {
		return 0, nil
	}

	// 写入前归一化 is_ac，读路径不再 UPPER(BTRIM(status))
	model.FillIsACBatch(tmp)

	// 只插入真正的新行，才能准确累加 daily_user_stats（OnConflict DoNothing 无法区分）
	ctx := context.Background()
	neu, err := dal.FilterNewSubmitLogs(ctx, uc.data.DB, tmp)
	if err != nil {
		return 0, err
	}
	if len(neu) == 0 {
		return 0, nil
	}

	res := uc.data.DB.
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "submit_id"}},
			DoNothing: true,
		}).
		CreateInBatches(&neu, submitInsertBatchSize)
	if res.Error != nil {
		return 0, res.Error
	}
	// 日汇总增量（热力/提交时段读路径）
	if err := dal.ApplyDailyDeltas(ctx, uc.data.DB, dal.AggregateSubmitDeltas(neu)); err != nil {
		log.Warnf("Spider: apply daily stats failed user=%d: %v", userId, err)
	}
	// 个人 AC 去重预聚合（写入时提前算）
	if err := dal.ApplyUserACFromSubmits(ctx, uc.data.DB, neu); err != nil {
		log.Warnf("Spider: apply user AC preagg failed user=%d: %v", userId, err)
	}
	if res.RowsAffected > 0 {
		spidermetrics.IncRows(res.RowsAffected)
	}
	// 以过滤后新行数为准（DoNothing 边界下更稳）
	return int64(len(neu)), nil
}

// fetchAndSaveContest 拉取并写入比赛记录；返回是否有写入尝试（Save 无法可靠区分 RowsAffected，有数据即视为可能变更）
func (uc *SpiderUseCase) fetchAndSaveContest(userId int64, plat model.Platform, needAll bool) (bool, error) {
	p, ok := spider.Get(plat.Platform)
	if !ok {
		return false, fmt.Errorf("平台插件不存在")
	}
	sbFetch, ok := p.(spider.SubmitContestFetcher)
	if !ok {
		return false, fmt.Errorf("平台未实现 SubmitContestFetcher")
	}
	tmp, err := sbFetch.FetchContestLog(userId, plat.Username, needAll)
	if err != nil {
		return false, err
	}
	if len(tmp) == 0 {
		return false, nil
	}

	err = uc.data.DB.
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "contest_id"}, {Name: "user_id"}},
			DoNothing: true,
		}).
		CreateInBatches(&tmp, submitInsertBatchSize).Error
	return true, err
}

// loadOnePlatform 返回 (是否有数据变更, error)
func (uc *SpiderUseCase) loadOnePlatform(userId int64, plat model.Platform, needAll bool) (bool, error) {
	// needAll 全量：最多 3 次（原先 12 次会把 worker 占死、队列堆积）
	maxRetries := 1
	if needAll {
		maxRetries = 3
	}
	var anyChange bool
	for i := 0; i < maxRetries; i++ {
		rows, err := uc.fetchAndSave(userId, plat, needAll)
		if rows > 0 {
			anyChange = true
		}
		if contestChanged, contestErr := uc.fetchAndSaveContest(userId, plat, needAll); contestErr != nil {
			log.Errorf("Spider: fetchAndSaveContest %s %s 失败: %v", plat.Platform, plat.Username, contestErr)
		} else if contestChanged {
			anyChange = true
		}
		if err == nil {
			log.Infof("Spider: %s %s 成功 new_rows=%d", plat.Platform, plat.Username, rows)
			if anyChange && uc.problem != nil {
				uc.problem.BindSubmitsAfterSpider(userId)
			}
			return anyChange, nil
		}
		if strings.Contains(err.Error(), "平台") {
			log.Errorf(
				"Spider: %s %s 失败: %v",
				plat.Platform,
				plat.Username,
				err,
			)
			return anyChange, err
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
			return anyChange, err
		}
		time.Sleep(3 * time.Second)
	}
	return anyChange, fmt.Errorf("platform %s max retries exceeded", plat.Platform)
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

	// 2. 个人统计版本：只失效该用户 period/heatmap 缓存
	_ = rdb.Incr(ctx, fmt.Sprintf("statistic:user:%d:ver", userId)).Err()

	// 3. 组织/全站全局版本：节流 INCR，避免 50 用户 cron 轮询时缓存 thrash
	//    SetNX 成功才 bump，窗口内其它爬虫跳过全局失效
	ok, err := rdb.SetNX(ctx, "statistic:global:ver:lock", "1", globalCacheBumpMinInterval).Result()
	if err != nil {
		// Redis 异常时仍尝试 bump，保证正确性优先
		_ = rdb.Incr(ctx, "statistic:heatmap:global:ver").Err()
		_ = rdb.Incr(ctx, "statistic:period:global:ver").Err()
	} else if ok {
		_ = rdb.Incr(ctx, "statistic:heatmap:global:ver").Err()
		_ = rdb.Incr(ctx, "statistic:period:global:ver").Err()
	}

	_ = rdb.Incr(ctx, fmt.Sprintf("core:contest_log:user:%d:ver", userId)).Err()

	// 热用户：异步预热 period 缓存（读路径更快，2c4g 上仅高热度触发）
	go MaybeWarmUserPeriod(context.Background(), uc.data.DB, rdb, userId)
}
