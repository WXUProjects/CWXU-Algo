package model

import "time"

type SubmitLog struct {
	ID       uint      `gorm:"comment:ID"`
	UserID   int64     `gorm:"comment:用户ID"`
	SubmitID string    `gorm:"comment:提交ID"`
	Contest  string    `gorm:"comment:比赛名称"`
	Problem  string    `gorm:"comment:问题"`
	Lang     string    `gorm:"comment:语言"`
	Status   string    `gorm:"comment:状态"`
	Time     time.Time `gorm:"comment:提交时间"`
}
