package task

import (
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"
	"time"

	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
)

type CronTask struct {
	spider  *SpiderTask
	summary *SummaryTask
	db      *gorm.DB
}

func NewCronTask(spider *SpiderTask, data *data.Data, summary *SummaryTask) *CronTask {
	return &CronTask{
		spider:  spider,
		db:      data.DB,
		summary: summary,
	}
}

func (t *CronTask) getUserIds() []int64 {
	var userIds []int64
	t.db.Model(&model.Platform{}).
		Select("DISTINCT user_id").
		Pluck("user_id", &userIds)
	return userIds
}

func (t *CronTask) Do() {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	cr := cron.New(cron.WithLocation(loc))
	_, _ = cr.AddFunc("0 * * * *", func() {
		// 增量查询
		// 获取所有platform表中存在的userid
		userIds := t.getUserIds()
		for _, v := range userIds {
			t.spider.Do(v, false)
		}
	})
	_, _ = cr.AddFunc("0 8 * * *", func() {
		// 早8点进行一次总结
		userIds := t.getUserIds()
		for _, v := range userIds {
			t.summary.Do(v, "PersonalLastDay")
		}
	})
	_, _ = cr.AddFunc("0 6,9,12,15,18,21,24 * * *", func() {
		// 每6 9 12 15 18 21 24 进行一次总结
		userIds := t.getUserIds()
		for _, v := range userIds {
			t.summary.Do(v, "PersonalRecent")
		}
	})
	cr.Start()
}
