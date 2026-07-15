package model

import "gorm.io/gorm"

const (
	// DefaultGroupName 组织默认分组名称（替代旧「未分组」）
	DefaultGroupName = "默认分组"
	DefaultGroupDesc = "组织默认分组"
)

type Group struct {
	gorm.Model
	Name     *string `gorm:"column:name;type:varchar(255);comment:组名称"`
	Describe string  `gorm:"comment:组描述"`
	OrgID    uint    `gorm:"index;not null;default:0;comment:所属组织"`
	Users    []User  `gorm:"foreignKey:GroupId;references:ID"`
}

// IsDefaultGroup 是否为组织默认分组
func (g *Group) IsDefaultGroup() bool {
	if g == nil || g.Name == nil {
		return false
	}
	n := *g.Name
	return n == DefaultGroupName || n == "未分组"
}
