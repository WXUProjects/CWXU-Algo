package model

import "time"

type ContestLog struct {
	ID       uint   `gorm:"comment:ID"`
	Platform string `gorm:"comment:平台;uniqueIndex:idx_contest_plat_user_cid,priority:1;index:idx_contest_lookup,priority:1"`
	// 唯一键必须含 platform：力扣 weekly-contest-N 与其它平台 contest_id 可能撞号
	UserID      int64     `gorm:"comment:用户ID;uniqueIndex:idx_contest_plat_user_cid,priority:2"`
	ContestId   string    `gorm:"comment:比赛Id;uniqueIndex:idx_contest_plat_user_cid,priority:3;index:idx_contest_lookup,priority:2"`
	ContestName string    `gorm:"comment:比赛名称;index"`
	ContestUrl  string    `gorm:"comment:比赛链接"`
	Rank        int       `gorm:"comment:排名;index:idx_contest_lookup,priority:3"`
	TotalCount  int       `gorm:"comment:总题数"`
	AcCount     int       `gorm:"comment:过题数"`
	Time        time.Time `gorm:"comment:比赛时间;index"`
	// EndTime 仅爬虫内存传递官方结束时间（不落库）；写入 contest_calendars 后供展示/Infer 使用
	EndTime time.Time `gorm:"-"`
}
