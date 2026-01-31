package dal

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// StatisticDal 统计数据访问层
type StatisticDal struct {
	db  *gorm.DB
	rdb *redis.Client
}

// NewStatisticDal 创建统计数据访问层
func NewStatisticDal(db *gorm.DB, rdb *redis.Client) *StatisticDal {
	return &StatisticDal{
		db:  db,
		rdb: rdb,
	}
}

// DailyCount 热力图每日统计
type DailyCount struct {
	Day time.Time
	Cnt int64
}

// HeatmapQuery 查询热力图数据
func (d *StatisticDal) HeatmapQuery(ctx context.Context, startDate, endDate string, userId int64, isAc bool) ([]DailyCount, error) {
	sub := d.db.
		Table("submit_logs").
		Select("id, time")
	if isAc {
		sub = sub.Where("status ILIKE ? OR status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%", "%OK%")
	}
	if userId != 0 {
		sub = sub.Where("user_id = ?", userId)
	}

	var result []DailyCount
	err := d.db.
		Table(
			fmt.Sprintf(`
			(
				SELECT generate_series(
					'%s'::date,
					'%s'::date,
					INTERVAL '1 day'
				) AS day
			) days
			`, startDate, endDate)).
		Select(`
			days.day,
			COUNT(s.id) AS cnt
			`).
		Joins(`
			LEFT JOIN (?) s
			ON s.time >= days.day
		   AND s.time <  days.day + INTERVAL '1 day'
		`, sub).
		Group("days.day").
		Order("days.day").
		Scan(&result).Error

	return result, err
}

// PeriodSubmitCount 提交次数统计
type PeriodSubmitCount struct {
	Today     int64
	ThisWeek  int64
	LastWeek  int64
	ThisMonth int64
	LastMonth int64
	ThisYear  int64
	LastYear  int64
	Total     int64
}

// PeriodAcCount AC次数统计
type PeriodAcCount struct {
	Today     int64
	ThisWeek  int64
	LastWeek int64
	ThisMonth int64
	LastMonth int64
	ThisYear  int64
	LastYear  int64
	Total     int64
}

// GetPeriodCount 获取时间段统计数据
func (d *StatisticDal) GetPeriodCount(userId int64) (PeriodSubmitCount, PeriodAcCount, error) {
	now := time.Now()

	// 日期范围计算
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	thisWeekStart := getWeekStart(now)
	lastWeekStart := thisWeekStart.Add(-7 * 24 * time.Hour)
	thisMonthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	lastMonthStart := thisMonthStart.AddDate(0, -1, 0)
	thisYearStart := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, now.Location())
	lastYearStart := thisYearStart.AddDate(-1, 0, 0)

	// 提交次数统计
	submit := PeriodSubmitCount{
		Today:     d.countQuery(userId, todayStart, now),
		ThisWeek:  d.countQuery(userId, thisWeekStart, now),
		LastWeek:  d.countQuery(userId, lastWeekStart, thisWeekStart),
		ThisMonth: d.countQuery(userId, thisMonthStart, now),
		LastMonth: d.countQuery(userId, lastMonthStart, thisMonthStart),
		ThisYear:  d.countQuery(userId, thisYearStart, now),
		LastYear:  d.countQuery(userId, lastYearStart, thisYearStart),
		Total:     d.countQueryTotal(userId),
	}

	// AC 次数统计（去重）
	ac := PeriodAcCount{
		Today:     d.countAcDistinctQuery(userId, todayStart, now),
		ThisWeek:  d.countAcDistinctQuery(userId, thisWeekStart, now),
		LastWeek:  d.countAcDistinctQuery(userId, lastWeekStart, thisWeekStart),
		ThisMonth: d.countAcDistinctQuery(userId, thisMonthStart, now),
		LastMonth: d.countAcDistinctQuery(userId, lastMonthStart, thisMonthStart),
		ThisYear:  d.countAcDistinctQuery(userId, thisYearStart, now),
		LastYear:  d.countAcDistinctQuery(userId, lastYearStart, thisYearStart),
		Total:     d.countAcDistinctTotal(userId),
	}

	return submit, ac, nil
}

// RankItem 排行榜项
type RankItem struct {
	Rank   int64
	UserID int64
	Name   string
	Score  int64
}

// GetRank 获取排行榜数据
func (d *StatisticDal) GetRank(ctx context.Context, userId int64, timeType, scoreType string, groupId int64, page, pageSize int64) ([]RankItem, int64, error) {
	now := time.Now()
	var startTime time.Time
	var endTime = now

	// 时间范围计算
	switch timeType {
	case "日":
		startTime = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	case "周":
		startTime = getWeekStart(now)
	case "月":
		startTime = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	default:
		// 默认全部时间
		startTime = time.Time{}
		endTime = time.Now().Add(100 * 365 * 24 * time.Hour)
	}

	type RankQueryResult struct {
		Rank   int64
		UserID int64
		Name   string
		Score  int64
	}

	var results []RankQueryResult
	var total int64

	// 基础查询
	baseQuery := d.db.Table("submit_logs").
		Where("time >= ? AND time < ?", startTime, endTime)

	// 按用户ID筛选
	if userId != 0 {
		baseQuery = baseQuery.Where("user_id = ?", userId)
	}

	// 按分组筛选
	if groupId != -1 {
		baseQuery = baseQuery.Where("group_id = ?", groupId)
	}

	// 根据scoreType决定统计方式
	if scoreType == "ac" {
		// AC排行榜，按user_id, problem去重
		baseQuery = baseQuery.Where("status ILIKE ? OR status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%", "%OK%")
	}

	// 获取总数
	countQuery := d.db.Table("(SELECT user_id FROM submit_logs WHERE time >= ? AND time < ?", startTime, endTime)
	if userId != 0 {
		countQuery = countQuery.Where("user_id = ?", userId)
	}
	if groupId != -1 {
		countQuery = countQuery.Where("group_id = ?", groupId)
	}
	if scoreType == "ac" {
		countQuery = countQuery.Where("status ILIKE ? OR status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%", "%OK%")
	}
	countQuery = countQuery.Group("user_id")
	countQuery.Count(&total)

	// 分页
	offset := (page - 1) * pageSize

	// 执行查询
	var selectClause string
	if scoreType == "ac" {
		selectClause = "COUNT(DISTINCT problem)"
	} else {
		selectClause = "COUNT(*)"
	}

	err := d.db.Table("(?)", baseQuery).
		Select("user_id, name, "+selectClause+" as score").
		Group("user_id, name").
		Order("score DESC").
		Offset(int(offset)).
		Limit(int(pageSize)).
		Scan(&results).Error

	if err != nil {
		return nil, 0, err
	}

	// 计算排名
	items := make([]RankItem, len(results))
	for i, r := range results {
		items[i] = RankItem{
			Rank:   int64(offset) + int64(i+1),
			UserID: r.UserID,
			Name:   r.Name,
			Score:  r.Score,
		}
	}

	return items, total, nil
}

// countQuery 统计指定时间范围内的记录数
func (d *StatisticDal) countQuery(userId int64, start, end time.Time) int64 {
	var count int64
	query := d.db.Table("submit_logs").Where("time >= ? AND time < ?", start, end)
	if userId != -1 {
		query = query.Where("user_id = ?", userId)
	}
	query.Count(&count)
	return count
}

// countQueryTotal 统计所有记录数
func (d *StatisticDal) countQueryTotal(userId int64) int64 {
	var count int64
	query := d.db.Table("submit_logs")
	if userId != -1 {
		query = query.Where("user_id = ?", userId)
	}
	query.Count(&count)
	return count
}

// countAcDistinctQuery 统计指定时间范围内的 AC 记录数（按 user_id, platform, problem 去重）
func (d *StatisticDal) countAcDistinctQuery(userId int64, start, end time.Time) int64 {
	var count int64
	query := d.db.Table("submit_logs").
		Select("DISTINCT user_id, platform, problem").
		Where("status ILIKE ? OR status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%", "%OK%").
		Where("time >= ? AND time < ?", start, end)
	if userId != -1 {
		query = query.Where("user_id = ?", userId)
	}
	query.Count(&count)
	return count
}

// countAcDistinctTotal 统计所有 AC 记录数（按 user_id, platform, problem 去重）
func (d *StatisticDal) countAcDistinctTotal(userId int64) int64 {
	var count int64
	query := d.db.Table("submit_logs").
		Select("DISTINCT user_id, platform, problem").
		Where("status ILIKE ? OR status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%", "%OK%")
	if userId != -1 {
		query = query.Where("user_id = ?", userId)
	}
	query.Count(&count)
	return count
}

// getWeekStart 获取本周周一 00:00:00
func getWeekStart(t time.Time) time.Time {
	weekday := t.Weekday()
	if weekday == time.Sunday {
		weekday = 7
	}
	days := int(weekday - time.Monday)
	return t.AddDate(0, 0, -days).Truncate(24 * time.Hour)
}