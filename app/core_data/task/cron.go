package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/dal"
	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
)

// UserSyncPolicy 与 user 服务 GetSyncPolicies 对齐
type UserSyncPolicy struct {
	UserID               int64
	EnableSpider         bool
	EnableAISummary      bool
	EnableAIEmail        bool
	EnableAIWeeklyEmail  bool
	IsOrgStaff           bool
	EmailEnabled         bool
	EmailWeeklyEnabled   bool
	SpiderIntervalMin    int
	AISummaryIntervalMin int
}

type CronTask struct {
	spider  *SpiderTask
	summary *SummaryTask
	db      *gorm.DB
	rdb     *redis.Client
	reg     *discovery.Register
	cron    *cron.Cron
	stopCh  chan struct{}
	mu      sync.RWMutex
}

func NewCronTask(spider *SpiderTask, data *data.Data, summary *SummaryTask, reg *discovery.Register) *CronTask {
	return &CronTask{
		spider:  spider,
		db:      data.DB,
		rdb:     data.RDB,
		summary: summary,
		reg:     reg,
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

func (t *CronTask) fetchPolicies(userIds []int64) map[int64]UserSyncPolicy {
	out := make(map[int64]UserSyncPolicy, len(userIds))
	if len(userIds) == 0 {
		return out
	}
	// 默认：无策略时仍允许按 60/180 跑（兼容 user 服务不可用）
	for _, uid := range userIds {
		out[uid] = UserSyncPolicy{
			UserID:               uid,
			EnableSpider:         true,
			EnableAISummary:      true,
			EnableAIEmail:        false, // 策略服务不可用时不入队邮件
			EnableAIWeeklyEmail:  false,
			EmailEnabled:         false,
			EmailWeeklyEnabled:   false,
			SpiderIntervalMin:    60,
			AISummaryIntervalMin: 180,
		}
	}
	if t.reg == nil {
		return out
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpc.DialInsecure(
		ctx,
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery(t.reg.Reg.(registry.Discovery)),
		grpc.WithTimeout(20*time.Second),
	)
	if err != nil {
		log.Warnf("CronTask: dial user for policies: %v", err)
		return out
	}
	defer conn.Close()
	cli := profile.NewProfileClient(conn)
	// 分批，避免超大 body
	const batch = 200
	for i := 0; i < len(userIds); i += batch {
		j := i + batch
		if j > len(userIds) {
			j = len(userIds)
		}
		res, err := cli.GetSyncPolicies(ctx, &profile.GetSyncPoliciesReq{UserIds: userIds[i:j]})
		if err != nil {
			log.Warnf("CronTask: GetSyncPolicies: %v", err)
			continue
		}
		for _, p := range res.GetPolicies() {
			sp := int(p.GetSpiderIntervalMin())
			if sp <= 0 {
				sp = 60
			}
			ai := int(p.GetAiSummaryIntervalMin())
			if ai <= 0 {
				ai = 180
			}
			out[p.GetUserId()] = UserSyncPolicy{
				UserID:               p.GetUserId(),
				EnableSpider:         p.GetEnableSpider(),
				EnableAISummary:      p.GetEnableAiSummary(),
				EnableAIEmail:        p.GetEnableAiEmail(),
				EnableAIWeeklyEmail:  p.GetEnableAiWeeklyEmail(),
				IsOrgStaff:           p.GetIsOrgStaff(),
				EmailEnabled:         p.GetEmailEnabled(),
				EmailWeeklyEnabled:   p.GetEmailWeeklyEnabled(),
				SpiderIntervalMin:    sp,
				AISummaryIntervalMin: ai,
			}
		}
	}
	return out
}

func lastKey(kind string, userId int64) string {
	return fmt.Sprintf("cron:last:%s:%d", kind, userId)
}

func cronLockKey(kind string) string {
	return fmt.Sprintf("cron:lock:%s", kind)
}

// tryCronLock 多 core_data 实例下同一 tick 只跑一次（TTL < 调度间隔）
func (t *CronTask) tryCronLock(kind string, ttl time.Duration) bool {
	if t.rdb == nil {
		return true
	}
	if ttl <= 0 {
		ttl = 4 * time.Minute
	}
	ok, err := t.rdb.SetNX(context.Background(), cronLockKey(kind), "1", ttl).Result()
	if err != nil {
		// Redis 故障：跳过本轮定时入队，避免多副本把队列打满
		log.Warnf("CronTask: lock %s failed, skip tick: %v", kind, err)
		return false
	}
	if !ok {
		log.Debugf("CronTask: skip tick %s (another instance holds lock)", kind)
		return false
	}
	return true
}

// tryClaim 原子占用本用户本周期：interval 内只入队一次（多实例安全）
// Redis 故障时跳过，避免 stampede。
func (t *CronTask) tryClaim(kind string, userId int64, intervalMin int) bool {
	if intervalMin <= 0 {
		intervalMin = 60
	}
	if t.rdb == nil {
		return true
	}
	ttl := time.Duration(intervalMin) * time.Minute
	ok, err := t.rdb.SetNX(context.Background(), lastKey(kind, userId), time.Now().Format(time.RFC3339), ttl).Result()
	if err != nil {
		log.Warnf("CronTask: claim %s user=%d failed, skip: %v", kind, userId, err)
		return false
	}
	return ok
}

func (t *CronTask) runSpiderTick() {
	if !t.tryCronLock("spider", 4*time.Minute) {
		return
	}
	userIds := t.getBoundUserIds()
	policies := t.fetchPolicies(userIds)
	// 去重：bound 用户列表已 DISTINCT；策略 map 按 user 一条
	enqueued := 0
	skipped := 0
	seen := make(map[int64]struct{}, len(userIds))
	for _, uid := range userIds {
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		p := policies[uid]
		if !p.EnableSpider {
			skipped++
			continue
		}
		if !t.tryClaim("spider", uid, p.SpiderIntervalMin) {
			skipped++
			continue
		}
		t.spider.Do(uid, false)
		enqueued++
	}
	log.Infof("CronTask spider: bound=%d unique=%d enqueued=%d skipped=%d", len(userIds), len(seen), enqueued, skipped)
}

func (t *CronTask) runRecentSummaryTick() {
	if !t.tryCronLock("summary_recent", 4*time.Minute) {
		return
	}
	userIds := t.getBoundUserIds()
	policies := t.fetchPolicies(userIds)
	enqueued := 0
	skipped := 0
	seen := make(map[int64]struct{}, len(userIds))
	for _, uid := range userIds {
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		p := policies[uid]
		if !p.EnableAISummary {
			skipped++
			continue
		}
		if !t.tryClaim("summary_recent", uid, p.AISummaryIntervalMin) {
			skipped++
			continue
		}
		t.summary.Do(uid, "PersonalRecent")
		enqueued++
	}
	log.Infof("CronTask summary PersonalRecent: bound=%d unique=%d enqueued=%d skipped=%d", len(userIds), len(seen), enqueued, skipped)
}

func (t *CronTask) runDailySummaryTick() {
	if !t.tryCronLock("summary_mail", 30*time.Minute) {
		return
	}
	userIds := t.getBoundUserIds()
	policies := t.fetchPolicies(userIds)
	enqueued := 0
	weekly := 0
	seen := make(map[int64]struct{}, len(userIds))
	isMonday := time.Now().Weekday() == time.Monday
	for _, uid := range userIds {
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		p := policies[uid]
		// 日报：组织授权 + 个人开
		if p.EnableAIEmail && p.EmailEnabled {
			t.summary.Do(uid, "PersonalLastDay")
			enqueued++
		}
		// 周报：周一 + 组织 staff 授权 + 个人周报开
		if isMonday && p.EnableAIWeeklyEmail && p.EmailWeeklyEnabled {
			t.summary.Do(uid, "WeeklyStaff")
			weekly++
		}
	}
	log.Infof("CronTask mail: bound=%d unique=%d daily=%d weekly=%d monday=%v",
		len(userIds), len(seen), enqueued, weekly, isMonday)
}

func (t *CronTask) Do() {
	t.mu.Lock()
	defer t.mu.Unlock()

	loc, _ := time.LoadLocation("Asia/Shanghai")
	t.cron = cron.New(cron.WithLocation(loc))
	// 每 5 分钟扫一次：按用户有效间隔（多组织 MIN）判断是否到期，每人最多入队一次
	_, _ = t.cron.AddFunc("*/5 * * * *", func() {
		t.runSpiderTick()
	})
	_, _ = t.cron.AddFunc("30 7 * * *", func() {
		t.runDailySummaryTick()
	})
	_, _ = t.cron.AddFunc("*/5 * * * *", func() {
		t.runRecentSummaryTick()
	})
	// 比赛日历：每 12 小时爬取 cpolar + 力扣；每 5 分钟检查邮件提醒
	_, _ = t.cron.AddFunc("0 */12 * * *", func() {
		t.runCalendarCrawl()
	})
	_, _ = t.cron.AddFunc("*/5 * * * *", func() {
		t.runCalendarNotify()
	})
	// 每日裁剪 6 个月外 submit_logs（预聚合/账本不动）
	_, _ = t.cron.AddFunc("20 3 * * *", func() {
		t.runSubmitLogPrune()
	})
	// 启动后异步跑一次爬取，避免空库等到下一个 12h 点
	go func() {
		time.Sleep(8 * time.Second)
		t.runCalendarCrawl()
	}()
	// 下次推上线：启动后异步跑 submit 6 个月清洗（幂等，与发版绑定）
	go t.runSubmitRetentionMigrateOnce()
	t.cron.Start()
	log.Infof("CronTask started: spider/summary every 5m; calendar crawl 12h + notify 5m; submit prune 03:20")
}

// runSubmitRetentionMigrateOnce 回填写死层/账本并删除 6 个月外明细
func (t *CronTask) runSubmitRetentionMigrateOnce() {
	time.Sleep(15 * time.Second)
	if t.db == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()
	res, err := dal.RunSubmitRetentionMigrate(ctx, t.db, t.rdb, false)
	if err != nil {
		log.Errorf("submit_retention migrate failed: %v", err)
		return
	}
	if res != nil && res.Skipped {
		log.Infof("submit_retention migrate skipped (already done)")
	}
}

// runSubmitLogPrune 热表保留近 6 个月
func (t *CronTask) runSubmitLogPrune() {
	if !t.tryCronLock("submit_prune", 2*time.Hour) {
		return
	}
	if t.db == nil {
		return
	}
	n, err := dal.PruneColdSubmitLogs(context.Background(), t.db, time.Now(), 5000)
	if err != nil {
		log.Errorf("CronTask submit_prune failed: %v", err)
		return
	}
	if n > 0 {
		log.Infof("CronTask submit_prune deleted=%d", n)
	}
}
