package model

import "time"

// ContestUserProblem 用户在某场比赛的单题汇总（站内 XCPCIO 风格榜热路径）。
// 唯一键：platform + contest_id + user_id + external_id
type ContestUserProblem struct {
	ID         uint `gorm:"primaryKey"`
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Platform   string `gorm:"size:32;not null;uniqueIndex:idx_cup_plat_cid_uid_ext,priority:1;index:idx_cup_plat_cid,priority:1;comment:平台"`
	ContestID  string `gorm:"size:128;not null;uniqueIndex:idx_cup_plat_cid_uid_ext,priority:2;index:idx_cup_plat_cid,priority:2;comment:OJ比赛id"`
	UserID     int64  `gorm:"not null;uniqueIndex:idx_cup_plat_cid_uid_ext,priority:3;index:idx_cup_user,priority:1;comment:用户ID"`
	Label      string `gorm:"size:16;not null;default:'';comment:展示题号A/B/C"`
	ExternalID string `gorm:"size:128;not null;uniqueIndex:idx_cup_plat_cid_uid_ext,priority:4;comment:与提交/题目录一致"`
	// Status: AC | UPSOLVE | UPSOLVE_TRIED | TRIED | NONE（NONE 一般不落库）
	// UPSOLVE = 赛时未 AC、赛后首次 AC；仅展示不计分。
	Status      string     `gorm:"size:16;not null;default:'';index;comment:AC|UPSOLVE|UPSOLVE_TRIED|TRIED"`
	Attempts    int        `gorm:"not null;default:0;comment:尝试次数(AC前WA+1或总尝试)"`
	FirstACAt   *time.Time `gorm:"comment:首次AC绝对时间"`
	RelativeSec *int       `gorm:"comment:相对开赛秒(可空；补题格应为空)"`
	// ScoreDelta 力扣单题 credit 等；ICPC 可空
	ScoreDelta int `gorm:"not null;default:0;comment:单题得分增量"`
}

func (ContestUserProblem) TableName() string { return "contest_user_problems" }

const (
	ContestCellAC           = "AC"
	ContestCellUpsolve      = "UPSOLVE"
	ContestCellUpsolveTried = "UPSOLVE_TRIED"
	ContestCellTried        = "TRIED"
	ContestCellNone         = "NONE"
)

// ContestCellHasDetail 是否有可展示的逐题明细（含补题）。
func ContestCellHasDetail(status string) bool {
	switch status {
	case ContestCellAC, ContestCellUpsolve, ContestCellUpsolveTried, ContestCellTried:
		return true
	default:
		return false
	}
}
