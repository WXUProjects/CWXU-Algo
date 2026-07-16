package model

import "time"

// DailyUserStat 用户日汇总：热力 / 提交时段 / 提交排行的读路径，避免扫 submit_logs 明细。
// 目标规模：1w 日活 · 单机 2c4g；行量约 users×活跃天数，远小于明细 10w–千万级。
//
// SubmitCnt：真实提交次数（排除力扣 lc-ac / lc-prob，见 CountsTowardSubmitStat）
// AcCnt：AC 条数（is_ac，含合成 AC 与最近通过，与热力 isAc=true 一致）
type DailyUserStat struct {
	UserID    int64     `gorm:"primaryKey;comment:用户ID"`
	Day       time.Time `gorm:"primaryKey;type:date;comment:自然日"`
	SubmitCnt int64     `gorm:"not null;default:0;comment:提交次数"`
	AcCnt     int64     `gorm:"not null;default:0;comment:AC次数"`
}

func (DailyUserStat) TableName() string {
	return "daily_user_stats"
}
