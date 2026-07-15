package service

import (
	"context"
	"fmt"
	"time"

	"cwxu-algo/app/agent/internal/agent"
	"cwxu-algo/app/agent/internal/data"
	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/common/discovery"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/redis/go-redis/v9"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
)

type SummaryUseCase struct {
	chat     *agent.Chat
	mailConf *conf.SMTP
	reg      *registry.Registrar
	redis    *redis.Client
}

func NewSummaryUseCase(chat *agent.Chat, mailConf *conf.SMTP, reg *discovery.Register, redis *data.Data) *SummaryUseCase {
	return &SummaryUseCase{
		chat:     chat,
		mailConf: mailConf,
		reg:      &reg.Reg,
		redis:    redis.RDB,
	}
}

func (uc *SummaryUseCase) PersonalLastDay(userId int64) error {
	if !uc.checkEmailEnabled(userId) {
		log.Infof("用户 %d 已关闭邮件发送，跳过", userId)
		return nil
	}

	roleId := uc.checkRoleId(userId)
	// RoleID=2 教练：仅周一发周报（无队员日报）
	// RoleID=3 队长：日报 + 周一额外周报（教练+队员）
	// RoleID=1 管理员：日报 + 周一额外周报
	// RoleID=0 队员：只发日报
	if roleId == 2 {
		if time.Now().Weekday() == time.Monday {
			return uc.WeeklyReportForCoach(userId)
		}
		return nil
	}
	if (roleId == 1 || roleId == 3) && time.Now().Weekday() == time.Monday {
		if err := uc.WeeklyReportForCoach(userId); err != nil {
			log.Errorf("用户 %d 周报发送失败: %v", userId, err)
		}
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

	subject := fmt.Sprintf("【算法协会日报】%s · %s", formatCNDate(data.Yesterday), data.Name)
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

func (uc *SummaryUseCase) WeeklyReportForCoach(coachUserId int64) error {
	if !uc.checkEmailEnabled(coachUserId) {
		log.Infof("教练 %d 已关闭邮件发送，跳过周报", coachUserId)
		return nil
	}

	lockKey := fmt.Sprintf("agent:lock:summary:weekly:%d", coachUserId)
	if !uc.tryAcquireLock(context.Background(), lockKey, 5*time.Minute) {
		log.Infof("教练 %d 周报生成进行中，跳过", coachUserId)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	data, err := uc.loadWeeklyReportData(ctx, coachUserId)
	if err != nil {
		return err
	}

	msgs := []*model.ChatCompletionMessage{
		{
			Role: model.ChatMessageRoleSystem,
			Content: &model.ChatCompletionMessageContent{
				StringValue: volcengine.String(weeklySystemPrompt()),
			},
		},
		{
			Role: model.ChatMessageRoleUser,
			Content: &model.ChatCompletionMessageContent{
				StringValue: volcengine.String(weeklyUserPrompt(data)),
			},
		},
	}
	html, err := uc.chat.Complete(ctx, msgs)
	if err != nil {
		return fmt.Errorf("生成周报失败: %w", err)
	}

	subject := fmt.Sprintf("【算法协会周报】%s-%s", formatCNDate(data.WeekStart), formatCNDate(data.WeekEnd))
	if err := uc.sendHTMLEmail(data.CoachEmail, subject, html); err != nil {
		return fmt.Errorf("发送周报失败: %w", err)
	}
	log.Infof("教练 %d 周报已发送至 %s", coachUserId, data.CoachEmail)
	return nil
}

func formatCNDate(ymd string) string {
	t, err := time.Parse(dateLayout, ymd)
	if err != nil {
		return ymd
	}
	return t.Format("1月2日")
}
