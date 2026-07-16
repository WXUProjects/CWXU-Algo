package task

import (
	"context"
	"fmt"
	"html"
	"strings"
	"time"

	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/sitesettings"
	"cwxu-algo/app/core_data/internal/data/dal"
	"cwxu-algo/app/core_data/internal/data/model"
	calspider "cwxu-algo/app/core_data/internal/spider/calendar"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
)

const maxAdvanceMinutes = 4320 // 与 model 白名单最大值一致

func (t *CronTask) calDal() *dal.ContestCalendarDal {
	return dal.NewContestCalendarDalDB(t.db)
}

func (t *CronTask) runCalendarCrawl() {
	if !t.tryCronLock("calendar_crawl", 30*time.Minute) {
		return
	}
	items, errs := calspider.FetchAll()
	for _, e := range errs {
		log.Warnf("CronTask calendar crawl source error: %v", e)
	}
	if len(items) == 0 {
		log.Warnf("CronTask calendar crawl: empty result (errs=%d)", len(errs))
		return
	}
	n, err := t.calDal().UpsertItems(items)
	if err != nil {
		log.Errorf("CronTask calendar upsert: %v", err)
		return
	}
	keepBefore := time.Now().Add(-7 * 24 * time.Hour).Unix()
	deleted, _ := t.calDal().CleanupEnded(keepBefore)
	_, _ = t.calDal().CleanupNotifyLogs(time.Now().Add(-30 * 24 * time.Hour))
	log.Infof("CronTask calendar crawl: upserted=%d deleted_ended=%d item_in=%d errs=%d",
		n, deleted, len(items), len(errs))
}

func (t *CronTask) runCalendarNotify() {
	if !t.tryCronLock("calendar_notify", 4*time.Minute) {
		return
	}
	now := time.Now()
	nowUnix := now.Unix()
	maxSec := int64(maxAdvanceMinutes * 60)

	contests, err := t.calDal().ListUpcomingInWindow(nowUnix, maxSec)
	if err != nil {
		log.Errorf("CronTask calendar notify list contests: %v", err)
		return
	}
	if len(contests) == 0 {
		return
	}
	subs, err := t.calDal().ListEnabledSubs()
	if err != nil {
		log.Errorf("CronTask calendar notify list subs: %v", err)
		return
	}
	if len(subs) == 0 {
		return
	}

	// platform -> []sub, calendarID -> []sub
	byPlatform := make(map[string][]model.ContestCalendarSub)
	byContest := make(map[uint][]model.ContestCalendarSub)
	userIDs := make(map[int64]struct{})
	for _, s := range subs {
		userIDs[s.UserID] = struct{}{}
		if s.Scope == model.CalScopePlatform {
			byPlatform[s.Platform] = append(byPlatform[s.Platform], s)
		} else if s.Scope == model.CalScopeContest && s.CalendarID > 0 {
			byContest[s.CalendarID] = append(byContest[s.CalendarID], s)
		}
	}

	// site_configs 在 user 库；只读 Redis，勿传 core_data DB
	rt := sitesettings.Load(context.Background(), t.rdb, nil)
	sender := rt.MailSender()
	if sender == nil || !sender.Configured() {
		log.Warnf("CronTask calendar notify: SMTP empty (Redis miss or not published by user service), skip send")
		return
	}

	emailCache := make(map[int64]string)
	sent := 0
	skipped := 0
	const batchLimit = 200
	for _, c := range contests {
		matched := make([]model.ContestCalendarSub, 0, 8)
		matched = append(matched, byPlatform[c.Platform]...)
		matched = append(matched, byContest[c.ID]...)
		// 去重：同一用户同一 advance 只处理一次
		seen := map[string]struct{}{}
		for _, sub := range matched {
			key := fmt.Sprintf("%d:%d", sub.UserID, sub.AdvanceMinutes)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}

			notifyAt := c.StartTime - int64(sub.AdvanceMinutes)*60
			if nowUnix < notifyAt {
				continue
			}
			if nowUnix >= c.StartTime {
				continue
			}
			// 先原子占坑再发信，避免「SMTP 已投递但日志未写」导致下轮 cron 重发；
			// 多实例并发时也只有一个能 claim 成功。
			claimed, err := t.calDal().TryClaimNotifyLog(sub.UserID, c.ID, sub.AdvanceMinutes)
			if err != nil {
				log.Warnf("CronTask calendar claim user=%d contest=%d: %v", sub.UserID, c.ID, err)
				skipped++
				continue
			}
			if !claimed {
				skipped++
				continue
			}
			to, ok := emailCache[sub.UserID]
			if !ok {
				to = t.lookupEmail(sub.UserID)
				emailCache[sub.UserID] = to
			}
			if strings.TrimSpace(to) == "" {
				// 无邮箱：释放占坑，绑定后仍可收到提醒
				_ = t.calDal().DeleteNotifyLog(sub.UserID, c.ID, sub.AdvanceMinutes)
				skipped++
				continue
			}
			subject, body := buildCalendarMail(rt.SiteTitle, &c, sub.AdvanceMinutes)
			if err := sender.Send(to, subject, body); err != nil {
				log.Warnf("CronTask calendar mail user=%d contest=%d: %v", sub.UserID, c.ID, err)
				// SMTP 明确失败才释放，允许下次重试；成功则保留日志防重发
				_ = t.calDal().DeleteNotifyLog(sub.UserID, c.ID, sub.AdvanceMinutes)
				skipped++
				continue
			}
			sent++
			if sent >= batchLimit {
				log.Infof("CronTask calendar notify: hit batch limit %d sent=%d skipped=%d", batchLimit, sent, skipped)
				return
			}
		}
	}
	if sent > 0 || skipped > 0 {
		log.Infof("CronTask calendar notify: sent=%d skipped=%d contests=%d", sent, skipped, len(contests))
	}
	_ = userIDs
}

func (t *CronTask) lookupEmail(userID int64) string {
	if t.reg == nil || userID <= 0 {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialInsecure(
		ctx,
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery(t.reg.Reg.(registry.Discovery)),
		grpc.WithTimeout(8*time.Second),
	)
	if err != nil {
		log.Warnf("CronTask calendar email dial: %v", err)
		return ""
	}
	defer conn.Close()
	cli := profile.NewProfileClient(conn)
	res, err := cli.GetContactEmail(ctx, &profile.GetContactEmailReq{UserId: userID})
	if err != nil || res == nil {
		return ""
	}
	return strings.TrimSpace(res.GetEmail())
}

func buildCalendarMail(siteTitle string, c *model.ContestCalendar, advanceMin int) (subject, body string) {
	if siteTitle == "" {
		siteTitle = "GoAlgo"
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	start := time.Unix(c.StartTime, 0).In(loc).Format("2006-01-02 15:04")
	end := time.Unix(c.EndTime, 0).In(loc).Format("2006-01-02 15:04")
	advLabel := formatAdvance(advanceMin)
	name := html.EscapeString(c.Name)
	plat := html.EscapeString(c.PlatformName)
	if plat == "" {
		plat = html.EscapeString(c.Platform)
	}
	url := html.EscapeString(c.URL)
	subject = fmt.Sprintf("[%s] 比赛提醒：%s 将于 %s 开始", siteTitle, c.Name, start)
	body = fmt.Sprintf(`<!DOCTYPE html><html><body style="font-family:sans-serif;line-height:1.6;color:#222">
<p>你好，</p>
<p>你订阅的比赛即将开始（提前 <strong>%s</strong> 提醒）：</p>
<table style="border-collapse:collapse">
<tr><td style="padding:4px 12px 4px 0;color:#666">平台</td><td>%s</td></tr>
<tr><td style="padding:4px 12px 4px 0;color:#666">比赛</td><td><strong>%s</strong></td></tr>
<tr><td style="padding:4px 12px 4px 0;color:#666">开始</td><td>%s（北京时间）</td></tr>
<tr><td style="padding:4px 12px 4px 0;color:#666">结束</td><td>%s（北京时间）</td></tr>
</table>
<p><a href="%s" style="display:inline-block;margin-top:12px;padding:10px 18px;background:#2563eb;color:#fff;text-decoration:none;border-radius:6px">前往比赛页面</a></p>
<p style="color:#888;font-size:13px;margin-top:24px">管理订阅：登录 %s → 比赛 → 比赛日历。若不再需要提醒，可在页面中取消订阅。</p>
</body></html>`,
		html.EscapeString(advLabel), plat, name, start, end, url, html.EscapeString(siteTitle))
	return subject, body
}

func formatAdvance(m int) string {
	if m < 60 {
		return fmt.Sprintf("%d 分钟", m)
	}
	if m%1440 == 0 {
		return fmt.Sprintf("%d 天", m/1440)
	}
	if m%60 == 0 {
		return fmt.Sprintf("%d 小时", m/60)
	}
	return fmt.Sprintf("%d 分钟", m)
}
