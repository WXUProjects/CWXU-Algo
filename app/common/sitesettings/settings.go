package sitesettings

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/common/mail"
	secretutil "cwxu-algo/app/common/utils/secret"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const (
	RedisKey = "site:runtime_config:v1"
	RedisTTL = 5 * time.Minute
)

// Runtime 跨服务共享的运行时配置（可 JSON 缓存到 Redis）
type Runtime struct {
	SiteTitle         string `json:"siteTitle"`
	SMTPHost          string `json:"smtpHost"`
	SMTPPort          int    `json:"smtpPort"`
	SMTPUsername      string `json:"smtpUsername"`
	SMTPPassword      string `json:"smtpPassword"`
	SMTPFrom          string `json:"smtpFrom"`
	AgentModel        string `json:"agentModel"`
	AgentSecret       string `json:"agentSecret"`
	AiAnalyzeEndpoint string `json:"aiAnalyzeEndpoint"`
	AiAnalyzeModel    string `json:"aiAnalyzeModel"`
	AiAnalyzeSecret   string `json:"aiAnalyzeSecret"`
}

// Row 与 site_configs 表对齐（轻量，避免依赖 user/internal）
type Row struct {
	ID                uint   `gorm:"primaryKey"`
	SiteTitle         string `gorm:"column:site_title"`
	SMTPHost          string `gorm:"column:smtp_host"`
	SMTPPort          int    `gorm:"column:smtp_port"`
	SMTPUsername      string `gorm:"column:smtp_username"`
	SMTPPassword      string `gorm:"column:smtp_password"`
	SMTPFrom          string `gorm:"column:smtp_from"`
	AgentModel        string `gorm:"column:agent_model"`
	AgentSecret       string `gorm:"column:agent_secret"`
	AiAnalyzeEndpoint string `gorm:"column:ai_analyze_endpoint"`
	AiAnalyzeModel    string `gorm:"column:ai_analyze_model"`
	AiAnalyzeSecret   string `gorm:"column:ai_analyze_secret"`
}

func (Row) TableName() string { return "site_configs" }

func (r *Row) ToRuntime() *Runtime {
	if r == nil {
		return &Runtime{}
	}
	port := r.SMTPPort
	if port <= 0 {
		port = 465
	}
	title := strings.TrimSpace(r.SiteTitle)
	if title == "" {
		title = "GoAlgo"
	}
	decrypt := func(value string) string {
		plain, err := secretutil.Decrypt(value)
		if err != nil {
			return ""
		}
		return plain
	}
	return &Runtime{
		SiteTitle:         title,
		SMTPHost:          strings.TrimSpace(r.SMTPHost),
		SMTPPort:          port,
		SMTPUsername:      strings.TrimSpace(r.SMTPUsername),
		SMTPPassword:      decrypt(r.SMTPPassword),
		SMTPFrom:          strings.TrimSpace(r.SMTPFrom),
		AgentModel:        strings.TrimSpace(r.AgentModel),
		AgentSecret:       decrypt(r.AgentSecret),
		AiAnalyzeEndpoint: strings.TrimSpace(r.AiAnalyzeEndpoint),
		AiAnalyzeModel:    strings.TrimSpace(r.AiAnalyzeModel),
		AiAnalyzeSecret:   decrypt(r.AiAnalyzeSecret),
	}
}

// LoadFromDB 读 id=1
func LoadFromDB(db *gorm.DB) (*Runtime, error) {
	if db == nil {
		return &Runtime{}, nil
	}
	var row Row
	if err := db.First(&row, 1).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return &Runtime{SiteTitle: "GoAlgo"}, nil
		}
		return nil, err
	}
	return row.ToRuntime(), nil
}

// PublishRedis 写入 Redis 缓存
func PublishRedis(ctx context.Context, rdb *redis.Client, rt *Runtime) error {
	if rdb == nil || rt == nil {
		return nil
	}
	b, err := json.Marshal(rt)
	if err != nil {
		return err
	}
	return rdb.Set(ctx, RedisKey, b, RedisTTL).Err()
}

// LoadFromRedis
func LoadFromRedis(ctx context.Context, rdb *redis.Client) (*Runtime, error) {
	if rdb == nil {
		return nil, redis.Nil
	}
	b, err := rdb.Get(ctx, RedisKey).Bytes()
	if err != nil {
		return nil, err
	}
	var rt Runtime
	if err := json.Unmarshal(b, &rt); err != nil {
		return nil, err
	}
	return &rt, nil
}

// Load 优先 Redis，失败再读 DB 并回填
func Load(ctx context.Context, rdb *redis.Client, db *gorm.DB) *Runtime {
	if rt, err := LoadFromRedis(ctx, rdb); err == nil && rt != nil {
		return rt
	}
	rt, err := LoadFromDB(db)
	if err != nil || rt == nil {
		return &Runtime{SiteTitle: "GoAlgo"}
	}
	_ = PublishRedis(ctx, rdb, rt)
	return rt
}

// LoadPreferDB 以 DB 为准（user 服务内）
func LoadPreferDB(ctx context.Context, db *gorm.DB, rdb *redis.Client) *Runtime {
	rt, err := LoadFromDB(db)
	if err != nil || rt == nil {
		return &Runtime{SiteTitle: "GoAlgo"}
	}
	_ = PublishRedis(ctx, rdb, rt)
	return rt
}

func (rt *Runtime) SMTPConf() *conf.SMTP {
	if rt == nil {
		return &conf.SMTP{}
	}
	port := rt.SMTPPort
	if port <= 0 {
		port = 465
	}
	return &conf.SMTP{
		Host:     rt.SMTPHost,
		Port:     int32(port),
		Username: rt.SMTPUsername,
		Password: rt.SMTPPassword,
		From:     rt.SMTPFrom,
	}
}

func (rt *Runtime) MailSender() *mail.Sender {
	return mail.NewSender(rt.SMTPConf())
}

func (rt *Runtime) AgentConf() *conf.Agent {
	if rt == nil {
		return &conf.Agent{}
	}
	return &conf.Agent{Model: rt.AgentModel, Secret: rt.AgentSecret}
}

func (rt *Runtime) AiAnalyzeConf() *conf.AiAnalyze {
	if rt == nil {
		return &conf.AiAnalyze{}
	}
	return &conf.AiAnalyze{
		Endpoint: rt.AiAnalyzeEndpoint,
		Model:    rt.AiAnalyzeModel,
		Secret:   rt.AiAnalyzeSecret,
	}
}

// MergeFallback 用 yaml 兜底填空字段（仅迁移期）
func (rt *Runtime) MergeFallback(smtp *conf.SMTP, agent *conf.Agent, ai *conf.AiAnalyze) *Runtime {
	if rt == nil {
		rt = &Runtime{SiteTitle: "GoAlgo"}
	}
	if smtp != nil {
		if rt.SMTPHost == "" {
			rt.SMTPHost = smtp.Host
		}
		if rt.SMTPPort <= 0 && smtp.Port > 0 {
			rt.SMTPPort = int(smtp.Port)
		}
		if rt.SMTPUsername == "" {
			rt.SMTPUsername = smtp.Username
		}
		if rt.SMTPPassword == "" {
			rt.SMTPPassword = smtp.Password
		}
		if rt.SMTPFrom == "" {
			rt.SMTPFrom = smtp.From
		}
	}
	if agent != nil {
		if rt.AgentModel == "" {
			rt.AgentModel = agent.Model
		}
		if rt.AgentSecret == "" {
			rt.AgentSecret = agent.Secret
		}
	}
	if ai != nil {
		if rt.AiAnalyzeEndpoint == "" {
			rt.AiAnalyzeEndpoint = ai.Endpoint
		}
		if rt.AiAnalyzeModel == "" {
			rt.AiAnalyzeModel = ai.Model
		}
		if rt.AiAnalyzeSecret == "" {
			rt.AiAnalyzeSecret = ai.Secret
		}
	}
	return rt
}

func MaskSecret(s string) string {
	if strings.TrimSpace(s) == "" {
		return ""
	}
	return "••••••••"
}
