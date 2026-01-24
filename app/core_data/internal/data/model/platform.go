package model

type Platform struct {
	Id       int64  `gorm:"primaryKey"`
	UserID   int64  `gorm:"comment:用户ID"`
	Platform string `gorm:"comment:平台"`
	Username string `gorm:"comment:平台用户名"`
}
