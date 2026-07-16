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

// 与 profile SetSyncIntervals / 组织间隔配置一致
const (
	defaultSpiderIntervalMin = 60
	defaultAIIntervalMin     = 180
	minSyncIntervalMin       = 5
	maxSyncIntervalMin       = 7 * 24 * 60 // 10080
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
	mu      sync.Mutex
	running bool
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
		// Stop 返回 context，等待正在跑的 job 结束
		ctx := t.cron.Stop()
		select {
		case <-ctx.Done():
		case <-time.After(30 * time.Second):
			log.Warnf("CronTask: cron.Stop wait timeout")
		}
		t.cron = nil
	}
	select {
	case <-t.stopCh:
	default:
		close(t.stopCh)
	}
	t.running = false
}

// clampInterval 防御脏数据：<=0 用默认，否则夹到 [5, 10080]
func clampInterval(v, def int) int {
	if v <= 0 {
		return def
	}
	if v < minSyncIntervalMin {
		return minSyncIntervalMin
	}
	if v > maxSyncIntervalMin {
		return maxSyncIntervalMin
	}
	return v
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
			SpiderIntervalMin:    defaultSpiderIntervalMin,
			AISummaryIntervalMin: defaultAIIntervalMin,
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
			out[p.GetUserId()] = UserSyncPolicy{
				UserID:               p.GetUserId(),
				EnableSpider:         p.GetEnableSpider(),
				EnableAISummary:      p.GetEnableAiSummary(),
				EnableAIEmail:        p.GetEnableAiEmail(),
				EnableAIWeeklyEmail:  p.GetEnableAiWeeklyEmail(),
				IsOrgStaff:           p.GetIsOrgStaff(),
				EmailEnabled:         p.GetEmailEnabled(),
				EmailWeeklyEnabled:   p.GetEmailWeeklyEnabled(),
				SpiderIntervalMin:    clampInterval(int(p.GetSpiderIntervalMin()), defaultSpiderIntervalMin),
				AISummaryIntervalMin: clampInterval(int(p.GetAiSummaryIntervalMin()), defaultAIIntervalMin),
			}
		}
	}
	return out
}

func cronLockKey(kind string) string {
	return fmt.Sprintf("cron:lock:%s", kind)
}

// claimPeriodKey 按墙钟周期槽位占坑（同一 interval 周期内 key 固定）
func claimPeriodKey(kind string, userId int64, periodUnix int64) string {
	return fmt.Sprintf("cron:claim:%s:%d:%d", kind, userId, periodUnix)
}

// cronTZ 用户可见间隔对齐时区（与 cron.WithLocation 一致）
func cronTZ() *time.Location {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return time.Local
	}
	return loc
}

// intervalPeriodStart 将 now 对齐到「当前间隔周期」起点（墙钟整点网格，非启动时刻）。
// 以 Asia/Shanghai 的固定原点 2020-01-01 00:00 起，按 interval 分钟切槽：
// 例如 60 分 → 每小时 :00；5 分 → :00/:05/…；180 分 → 00:00/03:00/06:00…
func intervalPeriodStart(now time.Time, intervalMin int) time.Time {
	intervalMin = clampInterval(intervalMin, defaultSpiderIntervalMin)
	loc := cronTZ()
	now = now.In(loc)
	interval := time.Duration(intervalMin) * time.Minute
	origin := time.Date(2020, 1, 1, 0, 0, 0, 0, loc)
	if now.Before(origin) {
		return origin
	}
	elapsed := now.Sub(origin)
	n := elapsed / interval
	return origin.Add(n * interval)
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

// tryClaim 原子占用本用户「当前墙钟周期」：同一 interval 槽位只入队一次（多实例安全）。
// 周期从整点网格起算，与服务启动时间无关；Redis 故障时跳过，避免 stampede。
func (t *CronTask) tryClaim(kind string, userId int64, intervalMin int) bool {
	intervalMin = clampInterval(intervalMin, defaultSpiderIntervalMin)
	if t.rdb == nil {
		return true
	}
	now := time.Now()
	period := intervalPeriodStart(now, intervalMin)
	next := period.Add(time.Duration(intervalMin) * time.Minute)
	// TTL 到周期结束 + 1 分钟缓冲，避免边界 tick 重复；下一周期用新 key
	ttl := time.Until(next) + time.Minute
	if ttl < time.Minute {
		ttl = time.Minute
	}
	key := claimPeriodKey(kind, userId, period.Unix())
	ok, err := t.rdb.SetNX(context.Background(), key, period.Format(time.RFC3339), ttl).Result()
	if err != nil {
		log.Warnf("CronTask: claim %s user=%d failed, skip: %v", kind, userId, err)
		return false
	}
	return ok
}

// releaseClaim 入队失败时释放「当前墙钟周期」占用，同周期内下个 tick 可重试
func (t *CronTask) releaseClaim(kind string, userId int64, intervalMin int) {
	if t.rdb == nil {
		return
	}
	intervalMin = clampInterval(intervalMin, defaultSpiderIntervalMin)
	period := intervalPeriodStart(time.Now(), intervalMin)
	key := claimPeriodKey(kind, userId, period.Unix())
	if err := t.rdb.Del(context.Background(), key).Err(); err != nil {
		log.Warnf("CronTask: release claim %s user=%d: %v", kind, userId, err)
	}
}

func (t *CronTask) runSpiderTick() {
	if !t.tryCronLock("spider", 4*time.Minute) {
		return
	}
	userIds := t.getBoundUserIds()
	policies := t.fetchPolicies(userIds)
	// 去重：bound 用户列表已 DISTINCT；策略 map 按 user 一条
	var publishedUsers, dedupUsers, failedUsers, skipped, disabled int
	var publishedJobs int
	seen := make(map[int64]struct{}, len(userIds))
	for _, uid := range userIds {
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		p := policies[uid]
		if !p.EnableSpider {
			disabled++
			continue
		}
		if !t.tryClaim("spider", uid, p.SpiderIntervalMin) {
			skipped++
			continue
		}
		res := t.spider.Do(uid, false)
		if !res.KeepClaim() {
			t.releaseClaim("spider", uid, p.SpiderIntervalMin)
			failedUsers++
			continue
		}
		publishedJobs += res.Published
		if res.Published > 0 {
			publishedUsers++
		} else {
			// 全 dedup：任务已在途，claim 保留
			dedupUsers++
		}
	}
	log.Infof("CronTask spider: bound=%d unique=%d published_users=%d published_jobs=%d dedup_users=%d failed_release=%d interval_skip=%d disabled=%d",
		len(userIds), len(seen), publishedUsers, publishedJobs, dedupUsers, failedUsers, skipped, disabled)
}

func (t *CronTask) runRecentSummaryTick() {
	if !t.tryCronLock("summary_recent", 4*time.Minute) {
		return
	}
	userIds := t.getBoundUserIds()
	policies := t.fetchPolicies(userIds)
	var published, dedup, failed, skipped, disabled int
	seen := make(map[int64]struct{}, len(userIds))
	for _, uid := range userIds {
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		p := policies[uid]
		if !p.EnableAISummary {
			disabled++
			continue
		}
		if !t.tryClaim("summary_recent", uid, p.AISummaryIntervalMin) {
			skipped++
			continue
		}
		res := t.summary.Do(uid, "PersonalRecent")
		if !res.KeepClaim() {
			t.releaseClaim("summary_recent", uid, p.AISummaryIntervalMin)
			failed++
			continue
		}
		if res.Published {
			published++
		} else {
			dedup++
		}
	}
	log.Infof("CronTask summary PersonalRecent: bound=%d unique=%d published=%d dedup=%d failed_release=%d interval_skip=%d disabled=%d",
		len(userIds), len(seen), published, dedup, failed, skipped, disabled)
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
			if t.summary.Do(uid, "PersonalLastDay").Published {
				enqueued++
			}
		}
		// 周报：周一 + 组织 staff 授权 + 个人周报开
		if isMonday && p.EnableAIWeeklyEmail && p.EmailWeeklyEnabled {
			if t.summary.Do(uid, "WeeklyStaff").Published {
				weekly++
			}
		}
	}
	log.Infof("CronTask mail: bound=%d unique=%d daily=%d weekly=%d monday=%v",
		len(userIds), len(seen), enqueued, weekly, isMonday)
}

// Do 启动 cron 并阻塞到 Stop，供 runForever 使用（只应有一个存活实例）。
// panic 后 defer 会停掉 cron，runForever 可安全重启。
func (t *CronTask) Do() {
	t.mu.Lock()
	if t.running {
		// 已有实例在跑：挂起等待 stop，避免 runForever 每 5s 再 Start 泄漏
		stopCh := t.stopCh
		t.mu.Unlock()
		<-stopCh
		return
	}
	// Stop 后 stopCh 已关闭：重建以便本轮阻塞与下次 Stop
	select {
	case <-t.stopCh:
		t.stopCh = make(chan struct{})
	default:
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	c := cron.New(cron.WithLocation(loc))
	// 每 5 分钟扫一次：按用户有效间隔（多组织 MIN）判断是否到期，每人最多入队一次
	_, _ = c.AddFunc("*/5 * * * *", func() {
		t.runSpiderTick()
	})
	_, _ = c.AddFunc("30 7 * * *", func() {
		t.runDailySummaryTick()
	})
	_, _ = c.AddFunc("*/5 * * * *", func() {
		t.runRecentSummaryTick()
	})
	// 比赛日历：每 12 小时爬取 cpolar + 力扣；每 5 分钟检查邮件提醒
	_, _ = c.AddFunc("0 */12 * * *", func() {
		t.runCalendarCrawl()
	})
	_, _ = c.AddFunc("*/5 * * * *", func() {
		t.runCalendarNotify()
	})
	// 每日裁剪热窗外 submit_logs（默认近 4 个日历月；预聚合/账本不动）
	_, _ = c.AddFunc("20 3 * * *", func() {
		t.runSubmitLogPrune()
	})
	// 启动后异步跑一次爬取，避免空库等到下一个 12h 点
	go func() {
		time.Sleep(8 * time.Second)
		select {
		case <-t.stopCh:
			return
		default:
		}
		t.runCalendarCrawl()
	}()
	// 启动后异步跑 submit 热表清洗（幂等；已 hot_only 则 skip，日常靠 prune 收紧）
	go t.runSubmitRetentionMigrateOnce()
	c.Start()
	t.cron = c
	t.running = true
	stopCh := t.stopCh
	t.mu.Unlock()

	log.Infof("CronTask started: spider/summary every 5m; calendar crawl 12h + notify 5m; submit prune 03:20 (hot=%dm)",
		model.SubmitLogRetentionMonths)

	defer func() {
		t.mu.Lock()
		if t.cron != nil {
			ctx := t.cron.Stop()
			select {
			case <-ctx.Done():
			case <-time.After(30 * time.Second):
			}
			t.cron = nil
		}
		t.running = false
		t.mu.Unlock()
	}()

	<-stopCh
	log.Infof("CronTask stopped")
}

// runSubmitRetentionMigrateOnce 回填写死层/账本并删除热窗外明细（幂等；已完成则 skip）
func (t *CronTask) runSubmitRetentionMigrateOnce() {
	time.Sleep(15 * time.Second)
	select {
	case <-t.stopCh:
		return
	default:
	}
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

// runSubmitLogPrune 热表只保留 SubmitLogRetentionMonths 个日历月内明细
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
