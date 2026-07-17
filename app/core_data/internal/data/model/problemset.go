package model

import "time"

// 题单系统类型
const (
	ProblemsetKindFavorites = "favorites" // 我的收藏（系统，不因 AC 剔除）
	ProblemsetKindTodo      = "todo"      // 待做题单（系统，AC 后自动剔除）
	ProblemsetKindCustom    = "custom"    // 用户自建
)

// 题单可见性
const (
	ProblemsetVisPrivate  = "private"
	ProblemsetVisPassword = "password"
	ProblemsetVisPublic   = "public"
)

// Problemset 题单头
type Problemset struct {
	ID        uint `gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time

	OwnerID uint   `gorm:"not null;index:idx_ps_owner_kind,priority:1;comment:所有者"`
	Title   string `gorm:"size:200;not null;comment:标题"`
	// Description 题单描述（Markdown 纯文本即可）
	Description string `gorm:"type:text;comment:描述"`
	// Kind favorites|todo|custom
	Kind string `gorm:"size:16;not null;default:custom;index:idx_ps_owner_kind,priority:2;comment:系统类型"`
	// Visibility private|password|public
	Visibility   string `gorm:"size:16;not null;default:private;index;comment:可见性"`
	PasswordHash string `gorm:"size:255;comment:访问密码 bcrypt"`
	// LikeCount 冗余点赞数
	LikeCount int `gorm:"not null;default:0;comment:点赞数"`
	// ItemCount 冗余题目数
	ItemCount int `gorm:"not null;default:0;comment:题目数"`
}

func (Problemset) TableName() string { return "problemsets" }

// ProblemsetItem 题单项
type ProblemsetItem struct {
	ID           uint `gorm:"primaryKey"`
	CreatedAt    time.Time
	ProblemsetID uint `gorm:"not null;uniqueIndex:idx_psi_set_problem,priority:1;index:idx_psi_problem,priority:1;comment:题单id"`
	ProblemID    uint `gorm:"not null;uniqueIndex:idx_psi_set_problem,priority:2;index:idx_psi_problem,priority:2;comment:题目id"`
	// SortOrder 排序（越小越靠前）
	SortOrder int `gorm:"not null;default:0;comment:排序"`
}

func (ProblemsetItem) TableName() string { return "problemset_items" }

// ProblemsetLike 题单点赞（每用户每题单一条）
type ProblemsetLike struct {
	ID           uint      `gorm:"primaryKey"`
	CreatedAt    time.Time `gorm:"index"`
	UserID       uint      `gorm:"not null;uniqueIndex:idx_psl_user_set,priority:1;comment:用户"`
	ProblemsetID uint      `gorm:"not null;uniqueIndex:idx_psl_user_set,priority:2;index;comment:题单"`
}

func (ProblemsetLike) TableName() string { return "problemset_likes" }
