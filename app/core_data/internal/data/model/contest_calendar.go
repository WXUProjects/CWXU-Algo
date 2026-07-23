package model

import "time"

// ContestCalendar 近期/即将开始的公开赛程（与 contest_logs 参赛记录分离）
type ContestCalendar struct {
	ID           uint      `gorm:"primaryKey"`
	Platform     string    `gorm:"size:32;not null;uniqueIndex:idx_cal_plat_ext,priority:1;index:idx_cal_platform;index:idx_cal_start"`
	PlatformName string    `gorm:"size:64;not null;comment:平台展示名"`
	ExternalID   string    `gorm:"size:128;not null;uniqueIndex:idx_cal_plat_ext,priority:2;comment:源站比赛ID"`
	Name         string    `gorm:"size:512;not null;index"`
	URL          string    `gorm:"size:1024;not null"`
	StartTime    int64     `gorm:"not null;index:idx_cal_start;comment:开始Unix秒"`
	EndTime      int64     `gorm:"not null;index;comment:结束Unix秒"`
	Source       string    `gorm:"size:32;not null;comment:cpolar|leetcode"`
	IconURL      string    `gorm:"size:512"`
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (ContestCalendar) TableName() string { return "contest_calendars" }

// ContestCalendarSub 用户赛程邮件订阅（platform 整平台 或 contest 单场）
type ContestCalendarSub struct {
	ID             uint      `gorm:"primaryKey"`
	UserID         int64     `gorm:"not null;uniqueIndex:idx_cal_sub_key,priority:1;index"`
	Scope          string    `gorm:"size:16;not null;uniqueIndex:idx_cal_sub_key,priority:2;comment:platform|contest"`
	Platform       string    `gorm:"size:32;not null;uniqueIndex:idx_cal_sub_key,priority:3;index"`
	CalendarID     uint      `gorm:"not null;uniqueIndex:idx_cal_sub_key,priority:4;default:0;comment:scope=contest时指向日历行"`
	AdvanceMinutes int       `gorm:"not null;default:180;comment:提前分钟数（默认3小时）"`
	Enabled        bool      `gorm:"not null;default:true"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (ContestCalendarSub) TableName() string { return "contest_calendar_subs" }

// ContestCalendarNotifyLog 同一用户同一场同一提前量只发一次
type ContestCalendarNotifyLog struct {
	ID             uint      `gorm:"primaryKey"`
	UserID         int64     `gorm:"not null;uniqueIndex:idx_cal_notify,priority:1"`
	CalendarID     uint      `gorm:"not null;uniqueIndex:idx_cal_notify,priority:2"`
	AdvanceMinutes int       `gorm:"not null;uniqueIndex:idx_cal_notify,priority:3"`
	SentAt         time.Time `gorm:"not null"`
}

func (ContestCalendarNotifyLog) TableName() string { return "contest_calendar_notify_logs" }

// 订阅提前量白名单（分钟）
var CalendarAdvanceMinutes = []int{30, 60, 180, 360, 720, 1440, 2880, 4320}

func ValidCalendarAdvance(m int) bool {
	for _, v := range CalendarAdvanceMinutes {
		if v == m {
			return true
		}
	}
	return false
}

const (
	CalScopePlatform  = "platform"
	CalScopeContest   = "contest"
	CalSourceCpolar   = "cpolar"
	CalSourceLeetCode = "leetcode"
	CalSourceNowCoder = "nowcoder" // 比赛页/参赛历史官方起止时间
)
