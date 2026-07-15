package model

import "time"

// SiteConfig 站点品牌配置（单行 id=1）
type SiteConfig struct {
	ID        uint      `gorm:"primaryKey"`
	SiteTitle string    `gorm:"size:128;not null;default:Algo-CWUX"`
	SiteLogo  string    `gorm:"size:512"` // logo / 侧栏图标 URL
	Favicon   string    `gorm:"size:512"` // favicon URL
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

func (SiteConfig) TableName() string { return "site_configs" }
