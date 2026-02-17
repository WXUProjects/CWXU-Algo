package model

import "time"

type ContestLog struct {
	ID          uint      `gorm:"comment:ID"`
	Platform    string    `gorm:"comment:平台"`
	UserID      int64     `gorm:"comment:用户ID;index:idx_contest_user,unique"` // 修改这里
	ContestId   string    `gorm:"comment:比赛Id;index:idx_contest_user,unique"` // 修改这里
	ContestName string    `gorm:"comment:比赛名称;index"`
	ContestUrl  string    `gorm:"comment:比赛链接"`
	Rank        int       `gorm:"comment:排名"`
	TotalCount  int       `gorm:"comment:总题数"`
	AcCount     int       `gorm:"comment:过题数"`
	Time        time.Time `gorm:"comment:比赛时间"`
}
