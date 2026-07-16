package model

import "time"

// CountedSubmitID 已计入预聚合的 submit_id 账本。
// 永久保留，防止全量重爬对 daily/user_ac 双计。
type CountedSubmitID struct {
	SubmitID  string    `gorm:"primaryKey;size:256;comment:提交ID"`
	UserID    int64     `gorm:"not null;index:idx_counted_user_plat,priority:1;comment:用户ID"`
	Platform  string    `gorm:"size:64;not null;index:idx_counted_user_plat,priority:2;comment:OJ平台"`
	CreatedAt time.Time `gorm:"autoCreateTime;comment:计入时间"`
}

func (CountedSubmitID) TableName() string {
	return "counted_submit_ids"
}
