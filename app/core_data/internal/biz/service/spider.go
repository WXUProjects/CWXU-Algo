package service

import (
	"context"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"fmt"
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

func (uc *SpiderUseCase) loadOnePlatform(userId int64, plat model.Platform, needAll bool) {
	for {
		err := uc.fetchAndSave(userId, plat, needAll)
		if err == nil {
			return
		}
		log.Errorf(
			"Spider: %s %s 失败: %v",
			plat.Platform,
			plat.Username,
			err,
		)
		// needAll=false，不重试
		if !needAll {
			return
		}
		// needAll=true，无限重试
		time.Sleep(5 * time.Second)
	}
}
func (uc *SpiderUseCase) invalidateCache(userId int64) {
	ctx := context.Background()
	pipe := uc.data.RDB.Pipeline()

	pipe.Del(ctx, fmt.Sprintf("core:submit_log:user:%d", userId))
	pipe.Del(ctx, fmt.Sprintf("user:%d:lastSubmitTime", userId))
	pipe.Del(ctx, fmt.Sprintf("statistic:heatmap:%d:*", userId))
	pipe.Del(ctx, "statistic:heatmap:0:*")

	if _, err := pipe.Exec(ctx); err != nil {
		log.Errorf("invalidate cache failed: %v", err)
	}
}
