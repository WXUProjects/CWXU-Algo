package model

import (
	"gorm.io/gorm"
)

type User struct {
	gorm.Model
	Username     string `gorm:"comment:用户名"`
	Password    string `gorm:"comment:密码"`
	Avatar      string `gorm:"comment:头像"`
	Name        string `gorm:"comment:姓名"`
	Email       string `gorm:"comment:邮箱"`
	GroupId     int64  `gorm:"comment:组id"`
	Group       Group  `gorm:"foreignKey:GroupId;references:ID"`
	RoleID      int    `gorm:"comment:角色ID;default:0"` // 0队员 1管理员 2教练 3队长
	EmailEnabled bool  `gorm:"comment:邮件发送开关;default:true"`
}
