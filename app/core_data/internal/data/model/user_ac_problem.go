package model

import (
	"fmt"
	"strings"
	"time"
)

// UserACProblem 用户 AC 去重题（生涯）：每题一行，first_ac_at 为首次 AC 时间。
// 用于 Total 去重题数；写入时 ON CONFLICT DO NOTHING。
type UserACProblem struct {
	UserID     int64     `gorm:"primaryKey;comment:用户ID"`
	ProblemKey string    `gorm:"primaryKey;size:512;comment:去重键"`
	FirstACAt  time.Time `gorm:"not null;index:idx_uac_user_first,priority:2;comment:首次AC时间"`
}

func (UserACProblem) TableName() string { return "user_ac_problems" }

// UserACProblemDay 用户某日 AC 过的题（按题去重到日）。
// 时段「窗内是否 AC 过该题」= COUNT(DISTINCT problem_key) WHERE day∈窗，与 submit_logs DISTINCT 语义一致。
type UserACProblemDay struct {
	UserID     int64     `gorm:"primaryKey;comment:用户ID"`
	Day        time.Time `gorm:"primaryKey;type:date;comment:自然日"`
	ProblemKey string    `gorm:"primaryKey;size:512;comment:去重键"`
}

func (UserACProblemDay) TableName() string { return "user_ac_problem_days" }

// ACProblemKey 与 dal.acProblemKeySQL 对齐：p:id / e:platform:ext / n:platform:name
func ACProblemKey(platform, externalID, problem string, problemID *uint) string {
	if problemID != nil && *problemID != 0 {
		return fmt.Sprintf("p:%d", *problemID)
	}
	ext := strings.TrimSpace(externalID)
	if ext != "" {
		return "e:" + platform + ":" + ext
	}
	return "n:" + platform + ":" + problem
}

// ACProblemKeyFromLog 从提交记录生成去重键
func ACProblemKeyFromLog(l *SubmitLog) string {
	if l == nil {
		return "n::"
	}
	return ACProblemKey(l.Platform, l.ExternalID, l.Problem, l.ProblemID)
}
