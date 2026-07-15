package model

import "time"

type User struct {
	ID        uint      `gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	Username  string `gorm:"comment:用户名"`
	Password  string `gorm:"comment:密码"`
	Avatar    string `gorm:"comment:头像"`
	Name      string `gorm:"comment:全局昵称(非真实姓名)"`
	Email     string `gorm:"comment:邮箱"`
	GroupId   int64  `gorm:"comment:组id(兼容旧字段;组织内分组见 org_members.group_id)"`
	Group     Group  `gorm:"foreignKey:GroupId;references:ID"`
	RoleID    int    `gorm:"comment:角色ID兼容;default:0"` // 迁移后以 is_site_admin + org 为准
	IsSiteAdmin  bool `gorm:"default:false;comment:站点管理员"`
	CurrentOrgID uint `gorm:"default:0;comment:当前组织ID"`
	// EmailEnabled 个人日报邮件；默认关，且须组织 enable_ai_email 才可开
	EmailEnabled bool `gorm:"comment:个人日报邮件;default:false"`
	// EmailWeeklyEnabled 个人周报（教练/队长/组织管理员）；与日报独立
	EmailWeeklyEnabled bool `gorm:"comment:个人周报邮件;default:false"`
}
