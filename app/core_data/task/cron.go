package task

import (
	"sync"
	"time"

	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
)

type CronTask struct {
	spider  *SpiderTask
	summary *SummaryTask
	db      *gorm.DB
	cron    *cron.Cron
	stopCh  chan struct{}
	mu      sync.RWMutex
}

func NewCronTask(spider *SpiderTask, data *data.Data, summary *SummaryTask) *CronTask {
	return &CronTask{
		spider:  spider,
		db:      data.DB,
		summary: summary,
		stopCh:  make(chan struct{}),
	}
}

func (t *CronTask) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.cron != nil {
		t.cron.Stop()
		t.cron = nil
	}
	select {
	case <-t.stopCh:
	default:
		close(t.stopCh)
	}
}

// getBoundUserIds 仅返回 platform 表中已绑定 OJ 的用户（去重）
func (t *CronTask) getBoundUserIds() []int64 {
	var userIds []int64
	if err := t.db.Model(&model.Platform{}).
		Distinct("user_id").
		Pluck("user_id", &userIds).Error; err != nil {
		log.Errorf("CronTask: query bound users failed: %v", err)
		return nil
	}
	return userIds
}

func (t *CronTask) Do() {
	t.mu.Lock()
	defer t.mu.Unlock()

	loc, _ := time.LoadLocation("Asia/Shanghai")
	t.cron = cron.New(cron.WithLocation(loc))
	_, _ = t.cron.AddFunc("1 * * * *", func() {
		// 增量同步：只爬已绑定 OJ 的用户
		userIds := t.getBoundUserIds()
		log.Infof("CronTask spider incremental: bound_users=%d", len(userIds))
		for _, v := range userIds {
			t.spider.Do(v, false)
		}
	})
	_, _ = t.cron.AddFunc("30 7 * * *", func() {
		// 早 7:30 日总结：仅已绑定用户
		userIds := t.getBoundUserIds()
		log.Infof("CronTask summary PersonalLastDay: bound_users=%d", len(userIds))
		for _, v := range userIds {
			t.summary.Do(v, "PersonalLastDay")
		}
	})
	_, _ = t.cron.AddFunc("1 6,9,12,15,18,21,0 * * *", func() {
		// 时段总结：仅已绑定用户
		userIds := t.getBoundUserIds()
		log.Infof("CronTask summary PersonalRecent: bound_users=%d", len(userIds))
		for _, v := range userIds {
			t.summary.Do(v, "PersonalRecent")
		}
	})
	t.cron.Start()
}
