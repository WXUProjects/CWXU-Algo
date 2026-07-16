package model

import (
	"strings"
	"time"
)

type SubmitLog struct {
	ID         uint      `gorm:"comment:ID"`
	Platform   string    `gorm:"comment:平台"`
	UserID     int64     `gorm:"comment:用户ID;index;index:idx_submit_user_time,priority:1;index:idx_submit_user_isac_time,priority:1"`
	SubmitID   string    `gorm:"comment:提交ID;unique"`
	Contest    string    `gorm:"comment:比赛名称"`
	Problem    string    `gorm:"comment:问题"`
	Lang       string    `gorm:"comment:语言"`
	Status     string    `gorm:"size:64;comment:状态;index:idx_submit_status_time,priority:1"`
	// IsAC 写入时由 status 归一化；统计读路径只扫 is_ac，避免 UPPER(BTRIM(status)) 全表表达式
	IsAC       bool      `gorm:"column:is_ac;default:false;index;index:idx_submit_user_isac_time,priority:2;comment:是否AC"`
	Time       time.Time `gorm:"comment:提交时间;index;index:idx_submit_user_time,priority:2;index:idx_submit_status_time,priority:2;index:idx_submit_user_isac_time,priority:3"`
	ProblemID  *uint     `gorm:"comment:关联题库ID;index"`
	ExternalID string    `gorm:"comment:平台题号;size:128;index"`
}

// acceptedStatusNorm 归一化后的 AC 状态集合（大写/去空白）
var acceptedStatusNorm = map[string]struct{}{
	"AC":       {},
	"OK":       {},
	"ACCEPTED": {},
	"正确":       {},
	"答案正确":     {},
}

// IsAcceptedStatus 判断提交状态是否为 AC（兼容各 OJ 字面量）
func IsAcceptedStatus(status string) bool {
	s := strings.ToUpper(strings.TrimSpace(status))
	_, ok := acceptedStatusNorm[s]
	if ok {
		return true
	}
	// 中文不受 ToUpper 影响，再试一次原 trim
	_, ok = acceptedStatusNorm[strings.TrimSpace(status)]
	return ok
}

// IsPendingSubmitStatus 评测中 / 无终态（可被后续爬虫回写 status）
func IsPendingSubmitStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "", "TESTING", "PENDING", "JUDGING", "IN_QUEUE":
		return true
	default:
		return false
	}
}

// FillIsAC 根据 Status 填充 IsAC（写入前调用）
func (s *SubmitLog) FillIsAC() {
	s.IsAC = IsAcceptedStatus(s.Status)
}

// FillIsACBatch 批量填充
func FillIsACBatch(logs []SubmitLog) {
	for i := range logs {
		logs[i].FillIsAC()
	}
}

// 力扣 submit_id 前缀约定（见 spider/platform/leetcode.go）：
//   lc-cal-*  日历提交次数 → 计入提交统计；不进动态
//   lc-pad-*  生涯提交补齐 → 计入提交统计；不进动态
//   lc-ac-*   合成 AC（无题号）→ 不计提交，计 AC；不进动态
//   lc-prob-* 最近通过明细 → 不计提交（避免与日历双计）；计 AC + 题库；**进动态/提交历史**（无代码）

// CountsTowardSubmitStat 是否计入提交次数 / 提交热力
// 力扣仅 lc-cal / lc-pad 计入；lc-ac / lc-prob 只服务 AC 与题库（lc-prob 另进活动流）。
func CountsTowardSubmitStat(platform, submitID string) bool {
	if platform != "LeetCode" {
		return true
	}
	return !IsLeetCodeNonSubmitCountID(submitID)
}

// IsLeetCodeNonSubmitCountID 力扣不计入提交数的 submit_id（合成 AC + 最近通过明细）
func IsLeetCodeNonSubmitCountID(submitID string) bool {
	return strings.HasPrefix(submitID, "lc-ac-") || strings.HasPrefix(submitID, "lc-prob-")
}

// IsLeetCodeSyntheticSubmit 力扣合成/补齐行：不进活动流与提交明细列表
// 最近通过 lc-prob-* 返回 false（应展示）。
func IsLeetCodeSyntheticSubmit(platform, submitID string) bool {
	if platform != "LeetCode" {
		return false
	}
	// 仅真实最近通过进动态；其余 lc-* 均为合成
	if strings.HasPrefix(submitID, "lc-prob-") {
		return false
	}
	return true
}

// SQLExcludeLeetCodeNonSubmit 提交统计 SQL 片段：排除力扣合成 AC 与最近通过明细
const SQLExcludeLeetCodeNonSubmit = `NOT (platform = 'LeetCode' AND (submit_id LIKE 'lc-ac-%' OR submit_id LIKE 'lc-prob-%'))`
