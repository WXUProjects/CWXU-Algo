package model

import "time"

const (
	OrgRoleMember   = "member"
	OrgRoleCoach    = "coach"
	OrgRoleCaptain  = "captain"
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
	ID        uint `gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	Name      string     `gorm:"size:128;not null;comment:组织名称"`
	Slug      string     `gorm:"size:64;uniqueIndex;comment:URL 标识"`
	Plan      string     `gorm:"size:32;default:free;comment:套餐 free|team|pro"`
	SeatLimit int        `gorm:"default:50;comment:用户数上限(席位)；默认50；公共域仅计仅属公共域的用户"`
	ExpireAt  *time.Time `gorm:"comment:套餐到期"`
	Status    string     `gorm:"size:16;default:active;comment:active|suspended"`
	IsSystem  bool       `gorm:"default:false;comment:系统组织(公共域)"`

	BrandTitle   string `gorm:"size:128;comment:组织品牌标题"`
	BrandLogo    string `gorm:"size:512;comment:组织 logo"`
	BrandFavicon string `gorm:"size:512;comment:组织 favicon"`

	JoinMode   string `gorm:"size:16;default:auto;comment:auto|review"`
	InviteCode string `gorm:"size:32;uniqueIndex;comment:团队识别码"`

	// 策略：开关可由组织管理员改；间隔仅站点管理员可写
	EnableAISummary     bool `gorm:"default:true;comment:AI总结开关(网页)"`
	EnableAIEmail       bool `gorm:"default:true;comment:AI日报邮件(组织授权)"`
	EnableAIWeeklyEmail bool `gorm:"default:true;comment:AI周报邮件(组织授权,staff)"`
	EnableSpider        bool `gorm:"default:true;comment:爬虫定时开关"`

	SpiderIntervalMin    int    `gorm:"default:60;comment:爬虫间隔分钟(站点写)"`
	AISummaryIntervalMin int    `gorm:"default:180;comment:AI总结间隔分钟(站点写)"`
	AIEmailSchedule      string `gorm:"size:64;default:30 7 * * *;comment:邮件 cron(站点写)"`

	DailySyncLimit int `gorm:"default:0;comment:组织日同步上限 0=未启用"`

	// ForceSync 本队强制同步（集训/比赛期跳过成员休眠）；仅站点管理员可写
	ForceSync bool `gorm:"default:false;comment:强制同步跳过休眠(站管)"`
}

// OrgMember 用户与组织关系
type OrgMember struct {
	ID             uint `gorm:"primaryKey"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
	OrgID          uint      `gorm:"uniqueIndex:idx_org_user;not null;comment:组织ID"`
	UserID         uint      `gorm:"uniqueIndex:idx_org_user;index;not null;comment:用户ID"`
	Role           string    `gorm:"size:16;default:member;comment:member|coach|captain|org_admin"`
	GroupID        *uint     `gorm:"index;comment:组织内分组"`
	OrgDisplayName string    `gorm:"size:64;comment:组织内名称(仅本组织展示)"`
	JoinedAt       time.Time `gorm:"comment:加入时间"`
}

// ValidOrgRole 组织内角色是否合法
func ValidOrgRole(role string) bool {
	switch role {
	case OrgRoleMember, OrgRoleCoach, OrgRoleCaptain, OrgRoleOrgAdmin:
		return true
	default:
		return false
	}
}

// IsOrgStaffRole 组织内可进管理端的角色（教练/队长/组织管理员）
func IsOrgStaffRole(role string) bool {
	return role == OrgRoleCoach || role == OrgRoleCaptain || role == OrgRoleOrgAdmin
}

// OrgJoinRequest 团队识别码加入申请（join_mode=review）
type OrgJoinRequest struct {
	ID             uint `gorm:"primaryKey"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
	OrgID          uint   `gorm:"uniqueIndex:idx_org_join_user;not null"`
	UserID         uint   `gorm:"uniqueIndex:idx_org_join_user;not null"`
	Status         string `gorm:"size:16;default:pending;comment:pending|approved|rejected"`
	CodeUsed       string `gorm:"size:32"`
	OrgDisplayName string `gorm:"size:64;comment:申请时填写的组织内名称"`
	ReviewedBy     *uint  `gorm:"comment:审批人"`
}

// PlanQuota 套餐配额模板
// 表名须显式指定：GORM 默认 inflection 会把 PlanQuota 收成 plan_quota（非 plan_quotas）。
type PlanQuota struct {
	ID                uint `gorm:"primaryKey"`
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Plan              string `gorm:"size:32;uniqueIndex;not null"`
	SeatLimit         int    `gorm:"default:20"`
	DailySyncPerUser  int    `gorm:"default:24"`
	AISummaryPerMonth int    `gorm:"default:0"`
}

func (PlanQuota) TableName() string { return "plan_quota" }
