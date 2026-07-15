package model

import "time"

// SiteConfig 站点品牌配置（单行 id=1）
type SiteConfig struct {
	ID        uint      `gorm:"primaryKey"`
	SiteTitle string    `gorm:"size:128;not null;default:GoAlgo"`
	SiteLogo  string    `gorm:"size:512"`
	Favicon   string    `gorm:"size:512"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

func (SiteConfig) TableName() string { return "site_configs" }
