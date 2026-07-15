package model

import "time"

// SiteConfig 站点配置（单行 id=1）：品牌 + 业务密钥
type SiteConfig struct {
	ID        uint      `gorm:"primaryKey"`
	SiteTitle string    `gorm:"size:128;not null;default:GoAlgo"`
	SiteLogo  string    `gorm:"size:512"`
	Favicon   string    `gorm:"size:512"`
	// FooterIcp 页脚备案号，空则前端用默认
	FooterIcp string `gorm:"size:128;column:footer_icp"`
	// SMTP
	SMTPHost     string `gorm:"size:256;column:smtp_host"`
	SMTPPort     int    `gorm:"column:smtp_port;default:465"`
	SMTPUsername string `gorm:"size:256;column:smtp_username"`
	SMTPPassword string `gorm:"size:512;column:smtp_password"`
	SMTPFrom     string `gorm:"size:256;column:smtp_from"`
	// Agent（火山 Ark / 日报周报）
	AgentModel  string `gorm:"size:128;column:agent_model"`
	AgentSecret string `gorm:"size:512;column:agent_secret"`
	// 题库 AI 分析（OpenAI 兼容）
	AiAnalyzeEndpoint string `gorm:"size:512;column:ai_analyze_endpoint"`
	AiAnalyzeModel    string `gorm:"size:128;column:ai_analyze_model"`
	AiAnalyzeSecret   string `gorm:"size:512;column:ai_analyze_secret"`
	UpdatedAt         time.Time `gorm:"autoUpdateTime"`
}

func (SiteConfig) TableName() string { return "site_configs" }
