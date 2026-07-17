package model

import "time"

// ProblemComment 题目评论（全站可见，不做组织隔离）
type ProblemComment struct {
	ID        uint      `gorm:"primaryKey"`
	CreatedAt time.Time `gorm:"index"`
	UpdatedAt time.Time
	ProblemID uint   `gorm:"not null;index:idx_pc_problem_created,priority:1;comment:题目id"`
	UserID    uint   `gorm:"not null;index;comment:作者"`
	Content   string `gorm:"type:text;not null;comment:评论内容"`
}

func (ProblemComment) TableName() string {
	return "problem_comments"
}

// ProblemUserSolution 用户题解（全站可见；与 AI SolutionsMeta 无关）
type ProblemUserSolution struct {
	ID        uint      `gorm:"primaryKey"`
	CreatedAt time.Time `gorm:"index"`
	UpdatedAt time.Time
	ProblemID uint   `gorm:"not null;index:idx_pus_problem_created,priority:1;comment:题目id"`
	UserID    uint   `gorm:"not null;index;comment:作者"`
	Title     string `gorm:"size:200;not null;comment:题解标题"`
	ContentMD string `gorm:"type:text;not null;comment:Markdown 正文"`
}

func (ProblemUserSolution) TableName() string {
	return "problem_user_solutions"
}

// ActivityFeedType 发现页动态类型
const (
	ActivityTypeComment  = "comment"
	ActivityTypeSolution = "solution"
)

// ActivityFeed 发现页动态（按 org_id 隔离；发布时取作者当前组织）
type ActivityFeed struct {
	ID        uint      `gorm:"primaryKey"`
	CreatedAt time.Time `gorm:"index:idx_af_org_created,priority:2"`
	// OrgID 作者发布时所属组织
	OrgID uint `gorm:"not null;index:idx_af_org_created,priority:1;comment:组织id"`
	// UserID 作者
	UserID uint `gorm:"not null;index;comment:作者"`
	// Type comment|solution
	Type string `gorm:"size:16;not null;index;comment:动态类型"`
	// RefID 评论或题解 id
	RefID uint `gorm:"not null;comment:源记录id"`
	// ProblemID 关联题目
	ProblemID uint `gorm:"not null;index;comment:题目id"`
	// Title 展示标题（题解标题 / 评论摘要）
	Title string `gorm:"size:256;comment:标题"`
	// Excerpt 摘要
	Excerpt string `gorm:"size:512;comment:摘要"`
}

func (ActivityFeed) TableName() string {
	return "activity_feeds"
}
