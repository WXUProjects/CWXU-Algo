package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"cwxu-algo/app/agent/internal/agent/tool/utils"

	"github.com/go-kratos/kratos/v2/log"
)

const recentSummaryTTL = 4 * time.Hour

type recentSummaryPayload struct {
	Msg        []string `json:"msg"`
	UpdateTime int64    `json:"updateTime"`
}

func (uc *SummaryUseCase) newEmailTool() *utils.SendEmail {
	return utils.NewSendEmail(
		uc.mailConf.Host,
		int(uc.mailConf.Port),
		uc.mailConf.Username,
		uc.mailConf.Password,
		uc.mailConf.From,
	)
}

func (uc *SummaryUseCase) sendHTMLEmail(to, subject, body string) error {
	body = stripCodeFence(body)
	if strings.TrimSpace(body) == "" {
		return fmt.Errorf("邮件正文为空")
	}
	if to == "" {
		return fmt.Errorf("收件人为空")
	}
	return uc.newEmailTool().Handle(to, subject, body)
}

func (uc *SummaryUseCase) saveRecentSummary(ctx context.Context, userId int64, raw string) error {
	raw = stripCodeFence(raw)
	var payload recentSummaryPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return fmt.Errorf("解析近期总结 JSON 失败: %w; raw=%s", err, truncateRunes(raw, 200))
	}
	if len(payload.Msg) < 5 || len(payload.Msg) > 10 {
		return fmt.Errorf("msg 条数应在 5-10，实际 %d", len(payload.Msg))
	}
	for i, m := range payload.Msg {
		if utf8.RuneCountInString(m) > 40 {
			// 截断过长条目，避免前端撑破
			payload.Msg[i] = string([]rune(m)[:40])
		}
	}
	if payload.UpdateTime == 0 {
		payload.UpdateTime = time.Now().Unix()
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("agent:summary:%d:recent", userId)
	if err := uc.redis.Set(ctx, key, string(b), recentSummaryTTL).Err(); err != nil {
		return fmt.Errorf("写入 Redis 失败: %w", err)
	}
	log.Infof("近期总结已写入 %s ttl=%s", key, recentSummaryTTL)
	return nil
}

func (uc *SummaryUseCase) tryAcquireLock(ctx context.Context, key string, ttl time.Duration) bool {
	ok, err := uc.redis.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		log.Warnf("获取锁失败 %s: %v", key, err)
		return true // 锁失败不阻塞主流程
	}
	return ok
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}
