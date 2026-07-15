package model

import (
	"time"

	"gorm.io/gorm"
)

// Paste 代码/文本粘贴板（Pastebin）
type Paste struct {
	gorm.Model
	Slug     string     `gorm:"size:16;uniqueIndex;not null;comment:公开短链"`
	Title    string     `gorm:"size:200;comment:标题"`
	Content  string     `gorm:"type:text;not null;comment:正文"`
	Language string     `gorm:"size:64;default:text;comment:语法高亮语言"`
	UserID   uint       `gorm:"index;not null;comment:创建者"`
	ExpireAt *time.Time `gorm:"index;comment:过期时间，空表示不过期"`
}
