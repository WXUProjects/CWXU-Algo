package model

import "time"

const (
	UserProblemStatusAC    = "AC"
	UserProblemStatusTried = "TRIED"
)

// UserProblemStatus 用户对已绑定 problem_id 的做题状态（无行 = NONE）
// AC 优先，不可降级为 TRIED。
type UserProblemStatus struct {
	UserID    int64     `gorm:"primaryKey;comment:用户ID"`
	ProblemID uint      `gorm:"primaryKey;index:idx_ups_problem,priority:1;comment:题目ID"`
	Status    string    `gorm:"size:16;not null;comment:AC|TRIED"`
	UpdatedAt time.Time `gorm:"not null;comment:更新时间"`
}

func (UserProblemStatus) TableName() string { return "user_problem_status" }
