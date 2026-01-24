package task

import (
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"
	"time"

	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
)

type CronTask struct {
	spider *SpiderTask
	db     *gorm.DB
}

func NewCronTask(spider *SpiderTask, data *data.Data) *CronTask {
	return &CronTask{
		spider: spider,
		db:     data.DB,
	}
}

func (t *CronTask) Do() {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	cr := cron.New(cron.WithLocation(loc))
	_, _ = cr.AddFunc("0 * * * *", func() {
		// 增量查询
		// 获取所有platform表中存在的userid
		var userIds []int64
		t.db.Model(&model.Platform{}).
			Select("DISTINCT user_id").
			Pluck("user_id", &userIds)
		for _, v := range userIds {
			t.spider.Do(v, false)
		}
	})
	cr.Start()
}
