package model

import "time"

// EmergencyNotice 全站紧急弹窗通知
type EmergencyNotice struct {
	ID         int64     `gorm:"primaryKey;autoIncrement"`
	Title      string    `gorm:"not null;comment:标题"`
	Content    string    `gorm:"not null;type:text;comment:内容"`
	Enabled    bool      `gorm:"default:true;index;comment:是否生效"`
	SortOrder  int64     `gorm:"default:0;index;comment:展示顺序 越小越先"`
	AuthorID   int64     `gorm:"not null;index;comment:发布者ID"`
	AuthorName string    `gorm:"not null;comment:发布者姓名"`
	CreatedAt  time.Time `gorm:"autoCreateTime;comment:创建时间"`
	UpdatedAt  time.Time `gorm:"autoUpdateTime;comment:更新时间"`
}

func (EmergencyNotice) TableName() string {
	return "emergency_notices"
}
