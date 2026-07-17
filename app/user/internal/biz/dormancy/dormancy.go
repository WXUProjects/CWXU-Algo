// Package dormancy 不活跃用户休眠判定（暂停后台爬虫 / AI / 邮件 / 画像预热）。
package dormancy

import "time"

const (
	DefaultInactiveDays = 14
	MinInactiveDays     = 1
	MaxInactiveDays     = 365
)

// ClampInactiveDays 站点配置夹取
func ClampInactiveDays(v int) int {
	if v <= 0 {
		return DefaultInactiveDays
	}
	if v < MinInactiveDays {
		return MinInactiveDays
	}
	if v > MaxInactiveDays {
		return MaxInactiveDays
	}
	return v
}

// ExemptFlags 豁免休眠的条件（任一即可）
type ExemptFlags struct {
	IsSiteAdmin bool
	SyncExempt  bool // 站管手动
	IsOrgStaff  bool // coach/captain/org_admin
	ForceSync   bool // 所属任一组织 force_sync
	PaidPlan    bool // 所属任一 active 组织 plan∈{team,pro}
}

// IsExempt 是否跳过休眠判定
func IsExempt(f ExemptFlags) bool {
	return f.IsSiteAdmin || f.SyncExempt || f.IsOrgStaff || f.ForceSync || f.PaidPlan
}

// IsInactiveByTime 仅按 last_login + 阈值判断（不含豁免）
func IsInactiveByTime(lastLogin *time.Time, inactiveDays int, now time.Time) bool {
	days := ClampInactiveDays(inactiveDays)
	if lastLogin == nil || lastLogin.IsZero() {
		return true
	}
	cutoff := now.Add(-time.Duration(days) * 24 * time.Hour)
	return lastLogin.Before(cutoff)
}

// IsDormant 是否应暂停后台任务：不活跃且无豁免
func IsDormant(lastLogin *time.Time, inactiveDays int, f ExemptFlags, now time.Time) bool {
	if IsExempt(f) {
		return false
	}
	return IsInactiveByTime(lastLogin, inactiveDays, now)
}

// IsPaidPlan plan 是否视为高优先级（跳过休眠）
func IsPaidPlan(plan string) bool {
	switch plan {
	case "team", "pro":
		return true
	default:
		return false
	}
}
