package model

type Platform struct {
	Id       int64  `gorm:"primaryKey"`
	UserID   int64  `gorm:"uniqueIndex:idx_platform_user;comment:用户ID"`
	Platform string `gorm:"size:32;uniqueIndex:idx_platform_user;comment:平台"`
	Username string `gorm:"size:128;not null;comment:平台用户名"`
}
