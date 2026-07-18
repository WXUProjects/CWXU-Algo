package model

import "time"

// ContestProblemEnsure 每场比赛（platform+contest_id）只跑一次题目发现/题面入库。
// 与参赛用户数无关：第一个打开详情的人触发，后续直接读结果。
type ContestProblemEnsure struct {
	ID        uint `gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	Platform  string `gorm:"size:32;not null;uniqueIndex:idx_cpe_plat_cid,priority:1;comment:平台"`
	ContestID string `gorm:"size:128;not null;uniqueIndex:idx_cpe_plat_cid,priority:2;comment:OJ比赛id"`
	// Status: running | done | failed
	Status    string     `gorm:"size:16;not null;default:running;comment:running|done|failed"`
	ErrorMsg  string     `gorm:"size:512;comment:失败原因"`
	EnsuredAt *time.Time `gorm:"comment:完成时间"`
}

func (ContestProblemEnsure) TableName() string { return "contest_problem_ensures" }

// ContestProblem 比赛内题目目录（与 problems 表通过 problem_id / external_id 对齐）。
// external_id 必须与用户提交解析结果一致，保证绑题不重复。
type ContestProblem struct {
	ID         uint `gorm:"primaryKey"`
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Platform   string `gorm:"size:32;not null;uniqueIndex:idx_cp_plat_cid_label,priority:1;index:idx_cp_plat_cid,priority:1;comment:平台"`
	ContestID  string `gorm:"size:128;not null;uniqueIndex:idx_cp_plat_cid_label,priority:2;index:idx_cp_plat_cid,priority:2;comment:OJ比赛id"`
	Label      string `gorm:"size:16;not null;uniqueIndex:idx_cp_plat_cid_label,priority:3;comment:展示题号A/B/C"`
	SortOrder  int    `gorm:"not null;default:0;comment:排序"`
	ExternalID string `gorm:"size:128;not null;index;comment:与提交解析一致的external_id"`
	Title      string `gorm:"size:256;comment:题目标题"`
	URL        string `gorm:"size:512;comment:题面URL"`
	ProblemID  uint   `gorm:"not null;default:0;index;comment:problems.id"`
}

func (ContestProblem) TableName() string { return "contest_problems" }

const (
	ContestEnsureRunning = "running"
	ContestEnsureDone    = "done"
	ContestEnsureFailed  = "failed"
)
