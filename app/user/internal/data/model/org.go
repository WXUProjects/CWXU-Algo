package model

import (
	"time"

	"gorm.io/gorm"
)

const (
	OrgRoleMember   = "member"
	OrgRoleOrgAdmin = "org_admin"

	OrgJoinAuto   = "auto"
	OrgJoinReview = "review"

	OrgStatusActive    = "active"
	OrgStatusSuspended = "suspended"

	PublicOrgSlug = "public"
	PublicOrgName = "公共域"

	JoinReqPending  = "pending"
	JoinReqApproved = "approved"
	JoinReqRejected = "rejected"
)

// Org 组织/校队（含系统「公共域」）
type Org struct {
	gorm.Model
	Name      string     `gorm:"size:128;not null;comment:组织名称"`
	Slug      string     `gorm:"size:64;uniqueIndex;comment:URL 标识"`
	Plan      string     `gorm:"size:32;default:free;comment:套餐 free|team|pro"`
	SeatLimit int        `gorm:"default:0;comment:席位上限 0=未限制"`
	ExpireAt  *time.Time `gorm:"comment:套餐到期"`
	Status    string     `gorm:"size:16;default:active;comment:active|suspended"`
	IsSystem  bool       `gorm:"default:false;comment:系统组织(公共域)"`

	BrandTitle   string `gorm:"size:128;comment:组织品牌标题"`
	BrandLogo    string `gorm:"size:512;comment:组织 logo"`
	BrandFavicon string `gorm:"size:512;comment:组织 favicon"`

	JoinMode   string `gorm:"size:16;default:auto;comment:auto|review"`
	InviteCode string `gorm:"size:32;uniqueIndex;comment:团队识别码"`

	// 策略：开关可由组织管理员改；间隔仅站点管理员可写
	EnableAISummary bool `gorm:"default:true;comment:AI总结开关"`
	EnableAIEmail   bool `gorm:"default:true;comment:AI邮件开关"`
	EnableSpider    bool `gorm:"default:true;comment:爬虫定时开关"`

	SpiderIntervalMin   int    `gorm:"default:60;comment:爬虫间隔分钟(站点写)"`
	AISummaryIntervalMin int   `gorm:"default:180;comment:AI总结间隔分钟(站点写)"`
	AIEmailSchedule     string `gorm:"size:64;default:30 7 * * *;comment:邮件 cron(站点写)"`

	DailySyncLimit int `gorm:"default:0;comment:组织日同步上限 0=未启用"`
}

// OrgMember 用户与组织关系
type OrgMember struct {
	gorm.Model
	OrgID    uint       `gorm:"uniqueIndex:idx_org_user;not null;comment:组织ID"`
	UserID   uint       `gorm:"uniqueIndex:idx_org_user;index;not null;comment:用户ID"`
	Role     string     `gorm:"size:16;default:member;comment:member|org_admin"`
	GroupID  *uint      `gorm:"index;comment:组织内分组"`
	JoinedAt time.Time  `gorm:"comment:加入时间"`
}

// OrgJoinRequest 团队识别码加入申请（join_mode=review）
type OrgJoinRequest struct {
	gorm.Model
	OrgID     uint   `gorm:"index;not null"`
	UserID    uint   `gorm:"index;not null"`
	Status    string `gorm:"size:16;default:pending;comment:pending|approved|rejected"`
	CodeUsed  string `gorm:"size:32"`
	ReviewedBy *uint `gorm:"comment:审批人"`
}

// PlanQuota 套餐配额模板
type PlanQuota struct {
	gorm.Model
	Plan              string `gorm:"size:32;uniqueIndex;not null"`
	SeatLimit         int    `gorm:"default:20"`
	DailySyncPerUser  int    `gorm:"default:24"`
	AISummaryPerMonth int    `gorm:"default:0"`
}
