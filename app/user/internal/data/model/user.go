package model

import (
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type User struct {
	gorm.Model
	Username string         `gorm:"comment:用户名"`
	Password string         `gorm:"comment:密码"`
	Avatar   string         `gorm:"comment:头像"`
	Name     string         `gorm:"comment:姓名"`
	Email    string         `gorm:"comment:邮箱"`
	GroupId  int64          `gorm:"comment:组id"`
	Group    Group          `gorm:"foreignKey:GroupId;references:ID"` // 关联 Group 模型，用于 GORM 关联操作
	RoleIDs  datatypes.JSON `gorm:"column:role_ids;type:json;comment:角色ID列表"`
}
