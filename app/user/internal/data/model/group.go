package model

import "gorm.io/gorm"

type Group struct {
	gorm.Model
	Name     *string `gorm:"column:name;type:varchar(255);comment:组名称"`
	Describe string  `gorm:"comment:组描述"`
	OrgID    uint    `gorm:"index;not null;default:0;comment:所属组织"`
	Users    []User  `gorm:"foreignKey:GroupId;references:ID"`
}
