package service

import (
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/gorm/model"
	"cwxu-algo/app/core_data/internal/spider"

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
				log.Infof("爬取%s %s中", plat.Platform, plat.Username)
				tmp, err := sbFetch.FetchSubmitLog(userId, plat.Username, needAll)
				if err != nil {
					log.Error("爬取失败", err.Error())
				}
				submitLog = append(submitLog, tmp...)
			}
		}
	}
	uc.data.DB.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "submit_id"},
		},
		DoNothing: true,
	}).Save(&submitLog)
	return nil
}
