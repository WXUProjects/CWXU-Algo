package model

import "time"

// UserFollow 关注关系：Follower 关注 Followee
type UserFollow struct {
	ID         uint `gorm:"primaryKey"`
	CreatedAt  time.Time
	FollowerID uint `gorm:"not null;uniqueIndex:idx_follow_pair;index:idx_follow_follower;comment:关注者"`
	FolloweeID uint `gorm:"not null;uniqueIndex:idx_follow_pair;index:idx_follow_followee;comment:被关注者"`
}

func (UserFollow) TableName() string {
	return "user_follows"
}
