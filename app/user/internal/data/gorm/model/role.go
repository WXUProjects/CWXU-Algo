package model

type Role struct {
	ID   uint    `gorm:"comment:id"`
	Name *string `gorm:"column:name;type:varchar(255);comment:角色名称"`
	Code *string `gorm:"column:code;type:varchar(128);comment:角色标识"`
}
