package model

import "time"

// SiteVisitDaily 站点访问日汇总（PV / DAU / UV / 独立 IP）
type SiteVisitDaily struct {
	ID        uint      `gorm:"primaryKey"`
	Day       time.Time `gorm:"type:date;uniqueIndex;not null;comment:统计日(上海)"`
	PV        int64     `gorm:"not null;default:0;comment:页面浏览量PV"`
	DAU       int64     `gorm:"not null;default:0;comment:登录日活DAU"`
	UV        int64     `gorm:"not null;default:0;comment:独立访客UV"`
	UniqueIP  int64     `gorm:"not null;default:0;comment:独立IP数"`
	UpdatedAt time.Time
}
