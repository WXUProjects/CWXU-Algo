package model

import (
	"fmt"
	"strings"
	"time"
)

// UserACProblem 用户 AC 去重题（生涯）：每题一行，first_ac_at 为首次 AC 时间。
// platform 用于换绑时按平台剪掉；读路径不按平台过滤。
type UserACProblem struct {
	UserID     int64     `gorm:"primaryKey;comment:用户ID"`
	ProblemKey string    `gorm:"primaryKey;size:512;comment:去重键"`
	Platform   string    `gorm:"size:64;index:idx_uac_user_plat,priority:2;not null;default:'';comment:OJ平台"`
	FirstACAt  time.Time `gorm:"not null;index:idx_uac_user_first,priority:2;comment:首次AC时间"`
}

func (UserACProblem) TableName() string { return "user_ac_problems" }

// UserACProblemDay 用户某日 AC 过的题（按题去重到日）。
type UserACProblemDay struct {
	UserID     int64     `gorm:"primaryKey;comment:用户ID"`
	Day        time.Time `gorm:"primaryKey;type:date;comment:自然日"`
	ProblemKey string    `gorm:"primaryKey;size:512;comment:去重键"`
	Platform   string    `gorm:"size:64;index:idx_uac_day_plat,priority:2;not null;default:'';comment:OJ平台"`
}

func (UserACProblemDay) TableName() string { return "user_ac_problem_days" }

// ACProblemKey 与 dal.acProblemKeySQL 对齐：p:id / e:platform:ext / n:platform:name
// 牛客：problem 常为「数字题号 标题」且 external_id 尚未回填时，用首 token 作 e: 键，
// 避免与后续 p:{problem_id} / e:NowCoder:{id} 双计。
func ACProblemKey(platform, externalID, problem string, problemID *uint) string {
	if problemID != nil && *problemID != 0 {
		return fmt.Sprintf("p:%d", *problemID)
	}
	ext := strings.TrimSpace(externalID)
	if ext != "" {
		return "e:" + platform + ":" + ext
	}
	if platform == "NowCoder" {
		if tok := strings.Fields(strings.TrimSpace(problem)); len(tok) > 0 && isAllDigits(tok[0]) {
			return "e:NowCoder:" + tok[0]
		}
	}
	return "n:" + platform + ":" + problem
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ACProblemKeyFromLog 从提交记录生成去重键
func ACProblemKeyFromLog(l *SubmitLog) string {
	if l == nil {
		return "n::"
	}
	return ACProblemKey(l.Platform, l.ExternalID, l.Problem, l.ProblemID)
}

// 力扣官方合成 AC 去重键前缀（spider 写入 e:LeetCode:ac-0..acTotal-1，对应官方 acTotal）
const LeetCodeOfficialACKeyPrefix = "e:LeetCode:ac-"

// IsLeetCodeOfficialACKey 是否为力扣官方合成过题键（非 recentAC 明细 slug）
func IsLeetCodeOfficialACKey(problemKey string) bool {
	return strings.HasPrefix(problemKey, LeetCodeOfficialACKeyPrefix)
}
