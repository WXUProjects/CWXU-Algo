package task

import (
	"time"

	"github.com/robfig/cron/v3"
)

type CronTask struct {
	spider *SpiderTask
}

func NewCronTask(spider *SpiderTask) *CronTask {
	return &CronTask{
		spider: spider,
	}
}

func (t *CronTask) Do() {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	cr := cron.New(cron.WithLocation(loc))
	_, _ = cr.AddFunc("0 * * * *", func() {
		// 增量查询
		t.spider.Do(0, false)
		t.spider.Do(1, false)
	})
	cr.Start()
}
