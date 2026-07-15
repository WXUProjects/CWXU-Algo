package model

import (
	"time"

	"gorm.io/gorm"
)

// Org 组织/校队（商业化多租户地基；当前无对外 API）
// 账号全局唯一，通过 OrgMember 绑定到组织。
type Org struct {
	gorm.Model
	Name      string     `gorm:"size:128;not null;comment:组织名称"`
	Slug      string     `gorm:"size:64;uniqueIndex;comment:URL 标识"`
	Plan      string     `gorm:"size:32;default:free;comment:套餐 free|team|pro"`
	SeatLimit int        `gorm:"default:0;comment:席位上限 0=未限制(开发期)"`
	ExpireAt  *time.Time `gorm:"comment:套餐到期"`
	Status    string     `gorm:"size:16;default:active;comment:active|suspended|expired"`
	// 同步配额地基（默认 0=沿用全局策略，业务侧暂不强制）
	DailySyncLimit int `gorm:"default:0;comment:组织日同步上限 0=未启用"`
}

// OrgMember 用户与组织的多对多关系
type OrgMember struct {
	gorm.Model
	OrgID    uint   `gorm:"uniqueIndex:idx_org_user;not null;comment:组织ID"`
	UserID   uint   `gorm:"uniqueIndex:idx_org_user;index;not null;comment:用户ID"`
	Role     string `gorm:"size:16;default:member;comment:owner|coach|member"`
	JoinedAt time.Time `gorm:"comment:加入时间"`
}

// PlanQuota 套餐配额模板（内部配置表，当前仅占位）
type PlanQuota struct {
	gorm.Model
	Plan              string `gorm:"size:32;uniqueIndex;not null;comment:套餐名"`
	SeatLimit         int    `gorm:"default:20;comment:默认席位"`
	DailySyncPerUser  int    `gorm:"default:24;comment:每用户日同步次数"`
	AISummaryPerMonth int    `gorm:"default:0;comment:每月 AI 总结次数"`
}
