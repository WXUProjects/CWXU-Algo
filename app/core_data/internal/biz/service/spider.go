package service

import (
	"context"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"fmt"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm/clause"
)

type SpiderUseCase struct {
	data *data.Data
}

func NewSpiderUseCase(data *data.Data) *SpiderUseCase {
	return &SpiderUseCase{
		data: data,
	}
}

// LoadData 加载数据
func (uc *SpiderUseCase) LoadData(userId int64, needAll bool) error {
	// 无论如何，函数退出前一定删缓存
	defer uc.invalidateCache(userId)

	var platforms []model.Platform
	if err := uc.data.DB.Where("user_id = ?", userId).Find(&platforms).Error; err != nil {
		return err
	}

	for _, plat := range platforms {
		uc.loadOnePlatform(userId, plat, needAll)
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

	return uc.data.DB.
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "submit_id"}},
			DoNothing: true,
		}).
		Save(&tmp).Error
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

func (uc *SpiderUseCase) loadOnePlatform(userId int64, plat model.Platform, needAll bool) {
	// 限制最大重试次数
	maxRetries := 12
	for i := 0; i < maxRetries; i++ {
		err := uc.fetchAndSave(userId, plat, needAll)
		_ = uc.fetchAndSaveContest(userId, plat, needAll)
		if err == nil {
			log.Infof("Spider: %s %s 成功", plat.Platform, plat.Username)
			uc.invalidateCache(userId)
			return
		}
		if strings.Contains(err.Error(), "平台") {
			log.Errorf(
				"Spider: %s %s 失败: %v",
				plat.Platform,
				plat.Username,
				err,
			)
			return
		}
		log.Errorf(
			"Spider: %s %s 失败 (重试 %d/%d): %v",
			plat.Platform,
			plat.Username,
			i+1,
			maxRetries,
			err,
		)
		// needAll=false，不重试
		if !needAll {
			return
		}
		// needAll=true，重试最多12次
		time.Sleep(5 * time.Second)
	}
	log.Errorf(
		"Spider: %s %s 达到最大重试次数 %d",
		plat.Platform,
		plat.Username,
		maxRetries,
	)
}
func (uc *SpiderUseCase) invalidateCache(userId int64) {
	ctx := context.Background()
	rdb := uc.data.RDB

	// log.Infof("清理缓存")

	// 1. 精确 key，直接删
	_ = rdb.Del(
		ctx,
		fmt.Sprintf("core:submit_log:user:%d", userId),
		fmt.Sprintf("user:%d:lastSubmitTime", userId),
		fmt.Sprintf("statistic:period:%d", userId), // 用户统计缓存
		fmt.Sprintf("statistic:period:-1"),         // 全局统计缓存
		// Contest log 精确 key
		fmt.Sprintf("core:contest_log:user:%d", userId),
	).Err()

	// 2. 模糊前缀，必须 SCAN
	patterns := []string{
		fmt.Sprintf("statistic:heatmap:%d:*:*:*", userId),
		"statistic:heatmap:0:*:*:*",
		// Contest log 相关的模糊 key
		fmt.Sprintf("core:contest_log:user:%d:*", userId),
		"core:contest_log:detail:*",
	}

	for _, pattern := range patterns {
		iter := rdb.Scan(ctx, 0, pattern, 200).Iterator()
		for iter.Next(ctx) {
			key := iter.Val()
			// 用 UNLINK，异步删除，不阻塞
			_ = rdb.Unlink(ctx, key).Err()
		}
		if err := iter.Err(); err != nil {
			log.Errorf("scan pattern %s failed: %v", pattern, err)
		}
	}
}
