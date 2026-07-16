package model

import "time"

type SubmitLog struct {
	ID         uint      `gorm:"comment:ID"`
	Platform   string    `gorm:"comment:平台"`
	UserID     int64     `gorm:"comment:用户ID;index;index:idx_submit_user_time,priority:1"`
	SubmitID   string    `gorm:"comment:提交ID;unique"`
	Contest    string    `gorm:"comment:比赛名称"`
	Problem    string    `gorm:"comment:问题"`
	Lang       string    `gorm:"comment:语言"`
	Status     string    `gorm:"size:64;comment:状态;index:idx_submit_status_time,priority:1"`
	Time       time.Time `gorm:"comment:提交时间;index;index:idx_submit_user_time,priority:2;index:idx_submit_status_time,priority:2"`
	ProblemID  *uint     `gorm:"comment:关联题库ID;index"`
	ExternalID string    `gorm:"comment:平台题号;size:128;index"`
}
