package model

// UserTagAC 用户在某算法标签上的去重 AC 题数（画像雷达写时预聚合）
type UserTagAC struct {
	UserID int64  `gorm:"primaryKey;comment:用户ID"`
	Tag    string `gorm:"primaryKey;size:64;not null;comment:算法标签"`
	Count  int64  `gorm:"not null;default:0;comment:去重 AC 题数"`
}

func (UserTagAC) TableName() string { return "user_tag_ac" }
