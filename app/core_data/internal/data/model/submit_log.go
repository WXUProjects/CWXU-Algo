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
