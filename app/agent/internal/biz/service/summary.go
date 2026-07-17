package service

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	profile2 "cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/agent/internal/agent"
	"cwxu-algo/app/agent/internal/data"
	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/common/sitesettings"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/redis/go-redis/v9"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
)

type SummaryUseCase struct {
	chat     *agent.Chat
	yamlSMTP *conf.SMTP
	reg      *registry.Registrar
	redis    *redis.Client
}

func NewSummaryUseCase(chat *agent.Chat, mailConf *conf.SMTP, reg *discovery.Register, d *data.Data) *SummaryUseCase {
	return &SummaryUseCase{
		chat:     chat,
		yamlSMTP: mailConf,
		reg:      &reg.Reg,
		redis:    d.RDB,
	}
}

func (uc *SummaryUseCase) runtime(ctx context.Context) *sitesettings.Runtime {
	rt := sitesettings.Load(ctx, uc.redis, nil)
	return rt.MergeFallback(uc.yamlSMTP, nil, nil)
}

func (uc *SummaryUseCase) brandTitle(ctx context.Context) string {
	t := strings.TrimSpace(uc.runtime(ctx).SiteTitle)
	if t == "" {
		return "GoAlgo"
	}
	return t
}

// PersonalLastDay 仅发个人日报（周报见 WeeklyStaff）
func (uc *SummaryUseCase) PersonalLastDay(userId int64) error {
	if !uc.canSendDailyEmail(userId) {
		log.Infof("用户 %d 日报未开启或无组织授权，跳过", userId)
		return nil
	}

	lockKey := fmt.Sprintf("agent:lock:summary:daily:%d", userId)
	if !uc.tryAcquireLock(context.Background(), lockKey, 3*time.Minute) {
		log.Infof("用户 %d 日报生成进行中，跳过", userId)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	data, err := uc.loadDailyReportData(ctx, userId)
	if err != nil {
		return err
	}

	msgs := []*model.ChatCompletionMessage{
		{
			Role: model.ChatMessageRoleSystem,
			Content: &model.ChatCompletionMessageContent{
				StringValue: volcengine.String(dailySystemPrompt(data.Name)),
			},
		},
		{
			Role: model.ChatMessageRoleUser,
			Content: &model.ChatCompletionMessageContent{
				StringValue: volcengine.String(dailyUserPrompt(data)),
			},
		},
	}
	html, err := uc.chat.Complete(ctx, msgs)
	if err != nil {
		return fmt.Errorf("生成日报失败: %w", err)
	}

	subject := fmt.Sprintf("【%s 日报】%s · %s", uc.brandTitle(ctx), formatCNDate(data.Yesterday), data.Name)
	if err := uc.sendHTMLEmail(data.Email, subject, html); err != nil {
		return fmt.Errorf("发送日报失败: %w", err)
	}
	log.Infof("用户 %d 日报已发送至 %s", userId, data.Email)
	return nil
}

func (uc *SummaryUseCase) PersonalRecent(userId int64) error {
	// 网页 AI 总结，与邮件开关无关
	lockKey := fmt.Sprintf("agent:lock:summary:recent:%d", userId)
	if !uc.tryAcquireLock(context.Background(), lockKey, 3*time.Minute) {
		log.Infof("用户 %d 近期总结生成进行中，跳过", userId)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	data, err := uc.loadRecentReportData(ctx, userId)
	if err != nil {
		return err
	}

	msgs := []*model.ChatCompletionMessage{
		{
			Role: model.ChatMessageRoleSystem,
			Content: &model.ChatCompletionMessageContent{
				StringValue: volcengine.String(recentSystemPrompt()),
			},
		},
		{
			Role: model.ChatMessageRoleUser,
			Content: &model.ChatCompletionMessageContent{
				StringValue: volcengine.String(recentUserPrompt(data)),
			},
		},
	}
	raw, err := uc.chat.Complete(ctx, msgs)
	if err != nil {
		return fmt.Errorf("生成近期总结失败: %w", err)
	}
	if err := uc.saveRecentSummary(ctx, userId, raw); err != nil {
		// 重试一次：强调只输出 JSON
		retryMsgs := append(msgs, &model.ChatCompletionMessage{
			Role: model.ChatMessageRoleUser,
			Content: &model.ChatCompletionMessageContent{
				StringValue: volcengine.String("上一次输出无法解析。请只输出合法 JSON：{\"msg\":[...],\"updateTime\":" + fmt.Sprintf("%d", data.NowUnix) + "}，不要其它文字。"),
			},
		})
		raw2, err2 := uc.chat.Complete(ctx, retryMsgs)
		if err2 != nil {
			return fmt.Errorf("近期总结校验失败: %v; 重试失败: %w", err, err2)
		}
		if err3 := uc.saveRecentSummary(ctx, userId, raw2); err3 != nil {
			return fmt.Errorf("近期总结校验失败: %v; 重试仍失败: %w", err, err3)
		}
	}
	log.Infof("用户 %d 近期总结已生成", userId)
	return nil
}

// WeeklyStaff 组织教练/队长/组织管理员周报 = 上周训练报告（共享训练报告管道）
func (uc *SummaryUseCase) WeeklyStaff(userId int64) error {
	if !uc.canSendWeeklyEmail(userId) {
		log.Infof("用户 %d 周报未开启或无组织 staff 授权，跳过", userId)
		return nil
	}

	lockKey := fmt.Sprintf("agent:lock:summary:weekly:%d", userId)
	if !uc.tryAcquireLock(context.Background(), lockKey, 5*time.Minute) {
		log.Infof("用户 %d 周报生成进行中，跳过", userId)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	orgIDs := uc.userStaffOrgIDs(ctx, userId)
	if len(orgIDs) == 0 {
		return fmt.Errorf("用户 %d 无 staff 组织，无法生成周报", userId)
	}
	weekStart, weekEnd := LastWeekRange(time.Now())
	email := uc.userContactEmail(userId)
	if email == "" {
		return fmt.Errorf("用户 %d 未绑定邮箱", userId)
	}
	var lastErr error
	sent := 0
	for _, orgID := range orgIDs {
		html, data, err := uc.GenerateTrainingReportSync(ctx, StartTrainingReportParams{
			OrgID:     orgID,
			GroupID:   0,
			StartDate: weekStart.Format(dateLayout),
			EndDate:   weekEnd.Format(dateLayout),
			UseAI:     true,
			CreatedBy: userId,
			Source:    "weekly",
		})
		if err != nil {
			lastErr = err
			log.Warnf("weekly training report org=%d user=%d: %v", orgID, userId, err)
			continue
		}
		subject := fmt.Sprintf("【%s 周报】%s-%s", uc.brandTitle(ctx), formatCNDate(weekStart.Format(dateLayout)), formatCNDate(weekEnd.Format(dateLayout)))
		if jobID, e := uc.persistWeeklyAsJob(ctx, orgID, userId, weekStart, weekEnd, html, data); e != nil {
			log.Warnf("weekly persist job user=%d org=%d: %v", userId, orgID, e)
		} else if jobID != "" {
			html += fmt.Sprintf(`<hr><p style="font-size:12px;color:#666">本周报即上周训练报告，任务 %s，可在组织管理下载 PDF（24 小时内）。</p>`, jobID)
		}
		if err := uc.sendHTMLEmail(email, subject, html); err != nil {
			lastErr = err
			log.Warnf("weekly email org=%d user=%d: %v", orgID, userId, err)
			continue
		}
		sent++
		log.Infof("用户 %d 周报(训练报告)已发送至 %s org=%d range=%s~%s", userId, email, orgID, weekStart.Format(dateLayout), weekEnd.Format(dateLayout))
	}
	if sent == 0 {
		if lastErr != nil {
			return lastErr
		}
		return fmt.Errorf("用户 %d 周报未发送任何组织", userId)
	}
	return nil
}

func (uc *SummaryUseCase) userStaffOrgIDs(ctx context.Context, userId int64) []int64 {
	conn, err := uc.dialUser(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close()
	cli := profile2.NewProfileClient(conn)
	res, err := cli.GetStaffOrgIds(ctx, &profile2.GetStaffOrgIdsReq{UserId: userId})
	if err != nil || res == nil {
		log.Warnf("GetStaffOrgIds user=%d: %v", userId, err)
		return nil
	}
	return res.GetOrgIds()
}

// persistWeeklyAsJob 将周报产物写入训练报告 job，便于下载
func (uc *SummaryUseCase) persistWeeklyAsJob(ctx context.Context, orgID, userID int64, start, end time.Time, html string, data *TrainingReportData) (string, error) {
	if err := ensureReportDir(); err != nil {
		return "", err
	}
	jobID := newJobID()
	now := time.Now()
	htmlPath, pdfPath := jobArtifactPaths(jobID)
	if err := os.WriteFile(htmlPath, []byte(html), 0o644); err != nil {
		return "", err
	}
	pdf := RenderSimplePDF(data, uc.brandTitle(ctx))
	if err := os.WriteFile(pdfPath, pdf, 0o644); err != nil {
		return "", err
	}
	job := &TrainingReportJob{
		JobID:      jobID,
		Status:     ReportStatusDone,
		Progress:   100,
		Message:    "周报已生成",
		StartDate:  start.Format(dateLayout),
		EndDate:    end.Format(dateLayout),
		UseAI:      true,
		OrgID:      orgID,
		CreatedBy:  userID,
		CreatedAt:  now.Unix(),
		FinishedAt: now.Unix(),
		ExpiresAt:  now.Add(reportDownloadTTL).Unix(),
		HTMLPath:   htmlPath,
		PDFPath:    pdfPath,
		FileName:   fmt.Sprintf("weekly-report-%s-%s.pdf", start.Format(dateLayout), end.Format(dateLayout)),
		Source:     "weekly",
	}
	if err := uc.saveJob(ctx, job); err != nil {
		return "", err
	}
	return jobID, nil
}

// WeeklyReportForCoach 兼容旧调用名
func (uc *SummaryUseCase) WeeklyReportForCoach(coachUserId int64) error {
	return uc.WeeklyStaff(coachUserId)
}

func formatCNDate(ymd string) string {
	t, err := time.Parse(dateLayout, ymd)
	if err != nil {
		return ymd
	}
	return t.Format("1月2日")
}
