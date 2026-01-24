package service

import (
	"context"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"fmt"

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
	// 根据userId 获取对应平台信息
	platforms := make([]model.Platform, 0)
	uc.data.DB.Where("user_id = ?", userId).Find(&platforms)
	submitLog := make([]model.SubmitLog, 0)
	for _, plat := range platforms {
		// 爬取数据
		if p, ok := spider.Get(plat.Platform); ok {
			if sbFetch, ok := p.(spider.SubmitLogFetcher); ok {
				tmp, err := sbFetch.FetchSubmitLog(userId, plat.Username, needAll)
				if err != nil {
					log.Errorf("Spider: %s %s爬取失败", plat.Platform, plat.Username)
					continue
				}
				log.Infof("Spider: %s %s爬取成功", plat.Platform, plat.Username)
				submitLog = append(submitLog, tmp...)
			} else {
				log.Errorf("Spider: %s 平台没有实现 SubmitLogFetcher", p.Name())
			}
		} else {
			log.Errorf("Spider: %s 平台对应的插件", p.Name())
		}
	}
	uc.data.DB.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "submit_id"},
		},
		DoNothing: true,
	}).Save(&submitLog)
	// 使得缓存失效
	uc.data.RDB.Del(context.Background(), fmt.Sprintf("core:submit_log:user:%d", userId))
	return nil
}
