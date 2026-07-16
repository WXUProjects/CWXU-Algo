package model

import "time"

// CountedSubmitID 已计入预聚合的 submit_id 账本。
// 热表 submit_logs 仅保留近 4 个日历月；账本永久保留，防止再爬双计。
type CountedSubmitID struct {
	SubmitID  string    `gorm:"primaryKey;size:256;comment:提交ID"`
	UserID    int64     `gorm:"not null;index:idx_counted_user_plat,priority:1;comment:用户ID"`
	Platform  string    `gorm:"size:64;not null;index:idx_counted_user_plat,priority:2;comment:OJ平台"`
	CreatedAt time.Time `gorm:"autoCreateTime;comment:计入时间"`
}

func (CountedSubmitID) TableName() string {
	return "counted_submit_ids"
}

// SubmitLogRetentionMonths 热表明细保留月数（日历月）。
// 收紧后无需线上手动清洗：每日 cron prune 按新 cutoff 逐步删冷明细。
const SubmitLogRetentionMonths = 4

// SubmitLogHotCutoff 热表明细 cutoff：早于此的提交只进预聚合+账本，不进 submit_logs
func SubmitLogHotCutoff(now time.Time) time.Time {
	return now.AddDate(0, -SubmitLogRetentionMonths, 0)
}

// IsWithinSubmitLogHotWindow 是否写入热表明细
func IsWithinSubmitLogHotWindow(t, now time.Time) bool {
	return !t.Before(SubmitLogHotCutoff(now))
}
