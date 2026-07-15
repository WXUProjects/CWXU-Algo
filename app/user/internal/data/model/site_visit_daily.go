package model

import "time"

// SiteVisitDaily 站点访问日汇总（人次 / 日活 / 独立访客）
type SiteVisitDaily struct {
	ID        uint      `gorm:"primaryKey"`
	Day       time.Time `gorm:"type:date;uniqueIndex;not null;comment:统计日(上海)"`
	PV        int64     `gorm:"not null;default:0;comment:访问人次"`
	DAU       int64     `gorm:"not null;default:0;comment:登录日活"`
	UV        int64     `gorm:"not null;default:0;comment:独立访客"`
	UpdatedAt time.Time
}
