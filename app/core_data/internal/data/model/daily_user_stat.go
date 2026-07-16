package model

import "time"

// DailyUserStat 用户×平台×日汇总：热力 / 提交时段 / 提交排行读路径。
// PK 含 platform，换绑时可按平台整段剪掉；读侧 SUM 跨平台。
//
// SubmitCnt：真实提交次数（排除力扣 lc-ac / lc-prob，见 CountsTowardSubmitStat）
// AcCnt：AC 条数（is_ac，含合成 AC 与最近通过，与热力 isAc=true 一致）
type DailyUserStat struct {
	UserID    int64     `gorm:"primaryKey;comment:用户ID"`
	Day       time.Time `gorm:"primaryKey;type:date;comment:自然日"`
	Platform  string    `gorm:"primaryKey;size:64;comment:OJ平台"`
	SubmitCnt int64     `gorm:"not null;default:0;comment:提交次数"`
	AcCnt     int64     `gorm:"not null;default:0;comment:AC次数"`
}

func (DailyUserStat) TableName() string {
	return "daily_user_stats"
}
