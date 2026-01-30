package service

import (
	"context"
	"cwxu-algo/api/core/v1/statistic"
	data2 "cwxu-algo/app/common/data"
	"cwxu-algo/app/core_data/internal/data"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

type StatisticService struct {
	statistic.UnimplementedStatisticServer
	db  *gorm.DB
	rdb *redis.Client
}
type DailyCount struct {
	Day time.Time
	Cnt int64
}

type PeriodCountData struct {
	Submit SubmitCount
	Ac     AcCount
}

type SubmitCount struct {
	Today     int64
	ThisWeek  int64
	LastWeek  int64
	ThisMonth int64
	LastMonth int64
	ThisYear  int64
	LastYear  int64
	Total     int64
}

type AcCount struct {
	Today     int64
	ThisWeek  int64
	LastWeek  int64
	ThisMonth int64
	LastMonth int64
	ThisYear  int64
	LastYear  int64
	Total     int64
}

func (s StatisticService) Heatmap(ctx context.Context, req *statistic.HeatmapReq) (*statistic.HeatmapResp, error) {
	db := s.db
	sub := db.
		Table("submit_logs").
		Select("id, time")
	if req.StartDate == "" || req.EndDate == "" {
		return nil, errors.BadRequest("参数错误", "日期参数错误")
	}
	if req.IsAc {
		sub = sub.Where("status ILIKE ? OR status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%", "%OK%")
	}
	if req.UserId != 0 {
		sub = sub.Where("user_id = ?", req.UserId)
	}
	// Redis 缓存查询
	cacheKey := fmt.Sprintf("statistic:heatmap:%d:%s:%s:%t", req.UserId, req.StartDate, req.EndDate, req.IsAc)
	res, _, err := data2.GetCacheDal[[]DailyCount](ctx, s.rdb, cacheKey, func(data *[]DailyCount) error {
		err := db.
			Table(
				fmt.Sprintf(`
				(
					SELECT generate_series(
						'%s'::date,
						'%s'::date,
						INTERVAL '1 day'
					) AS day
				) days
				`, req.StartDate, req.EndDate)).
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
			Scan(&data).Error
		return err
	})
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}
	d := make([]*statistic.HeatmapResp_HeatmapItem, len(*res))
	for i, v := range *res {
		d[i] = &statistic.HeatmapResp_HeatmapItem{
			Date:  v.Day.Format("2006-01-02"),
			Count: v.Cnt,
		}
	}
	return &statistic.HeatmapResp{
		Code: 0,
		Data: d,
	}, nil
}

func (s StatisticService) PeriodCount(ctx context.Context, req *statistic.PeriodCountReq) (*statistic.PeriodCountResp, error) {
	cacheKey := fmt.Sprintf("statistic:period:%d", req.UserId)
	res, _, err := data2.GetCacheDal[PeriodCountData](ctx, s.rdb, cacheKey, func(data *PeriodCountData) error {
		now := time.Now()

		// 日期范围计算
		todayStart := now.Truncate(24 * time.Hour)
		thisWeekStart := getWeekStart(now)
		lastWeekStart := thisWeekStart.Add(-7 * 24 * time.Hour)
		thisMonthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
		lastMonthStart := thisMonthStart.AddDate(0, -1, 0)
		thisYearStart := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, now.Location())
		lastYearStart := thisYearStart.AddDate(-1, 0, 0)

		// 提交次数统计
		var submit SubmitCount
		submit.Today = countQuery(s.db, req.UserId, todayStart, now)
		submit.ThisWeek = countQuery(s.db, req.UserId, thisWeekStart, now)
		submit.LastWeek = countQuery(s.db, req.UserId, lastWeekStart, thisWeekStart)
		submit.ThisMonth = countQuery(s.db, req.UserId, thisMonthStart, now)
		submit.LastMonth = countQuery(s.db, req.UserId, lastMonthStart, thisMonthStart)
		submit.ThisYear = countQuery(s.db, req.UserId, thisYearStart, now)
		submit.LastYear = countQuery(s.db, req.UserId, lastYearStart, thisYearStart)
		submit.Total = countQueryTotal(s.db, req.UserId)

		// AC 次数统计（去重）
		var ac AcCount
		ac.Today = countAcDistinctQuery(s.db, req.UserId, todayStart, now)
		ac.ThisWeek = countAcDistinctQuery(s.db, req.UserId, thisWeekStart, now)
		ac.LastWeek = countAcDistinctQuery(s.db, req.UserId, lastWeekStart, thisWeekStart)
		ac.ThisMonth = countAcDistinctQuery(s.db, req.UserId, thisMonthStart, now)
		ac.LastMonth = countAcDistinctQuery(s.db, req.UserId, lastMonthStart, thisMonthStart)
		ac.ThisYear = countAcDistinctQuery(s.db, req.UserId, thisYearStart, now)
		ac.LastYear = countAcDistinctQuery(s.db, req.UserId, lastYearStart, thisYearStart)
		ac.Total = countAcDistinctTotal(s.db, req.UserId)

		data.Submit = submit
		data.Ac = ac
		return nil
	})

	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}

	return &statistic.PeriodCountResp{
		Code: 0,
		Data: &statistic.PeriodData{
			Submit: &statistic.SubmitCount{
				Today:     res.Submit.Today,
				ThisWeek:  res.Submit.ThisWeek,
				LastWeek:  res.Submit.LastWeek,
				ThisMonth: res.Submit.ThisMonth,
				LastMonth: res.Submit.LastMonth,
				ThisYear:  res.Submit.ThisYear,
				LastYear:  res.Submit.LastYear,
				Total:     res.Submit.Total,
			},
			Ac: &statistic.AcCount{
				Today:     res.Ac.Today,
				ThisWeek:  res.Ac.ThisWeek,
				LastWeek:  res.Ac.LastWeek,
				ThisMonth: res.Ac.ThisMonth,
				LastMonth: res.Ac.LastMonth,
				ThisYear:  res.Ac.ThisYear,
				LastYear:  res.Ac.LastYear,
				Total:     res.Ac.Total,
			},
		},
	}, nil
}

// countQuery 统计指定时间范围内的记录数
func countQuery(q *gorm.DB, userId int64, start, end time.Time) int64 {
	var count int64
	subQuery := q.Table("submit_logs").Where("time >= ? AND time < ?", start, end)
	if userId != -1 {
		subQuery = subQuery.Where("user_id = ?", userId)
	}
	subQuery.Count(&count)
	return count
}

// countQueryTotal 统计所有记录数
func countQueryTotal(q *gorm.DB, userId int64) int64 {
	var count int64
	subQuery := q.Table("submit_logs")
	if userId != -1 {
		subQuery = subQuery.Where("user_id = ?", userId)
	}
	subQuery.Count(&count)
	return count
}

// countAcDistinctQuery 统计指定时间范围内的 AC 记录数（按 user_id, platform, problem 去重）
func countAcDistinctQuery(q *gorm.DB, userId int64, start, end time.Time) int64 {
	var count int64
	// 使用子查询去重
	subQuery := q.Table("submit_logs").
		Select("DISTINCT user_id, platform, problem").
		Where("status ILIKE ? OR status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%", "%OK%").
		Where("time >= ? AND time < ?", start, end)
	if userId != -1 {
		subQuery = subQuery.Where("user_id = ?", userId)
	}
	subQuery.Count(&count)
	return count
}

// countAcDistinctTotal 统计所有 AC 记录数（按 user_id, platform, problem 去重）
func countAcDistinctTotal(q *gorm.DB, userId int64) int64 {
	var count int64
	subQuery := q.Table("submit_logs").
		Select("DISTINCT user_id, platform, problem").
		Where("status ILIKE ? OR status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%", "%OK%")
	if userId != -1 {
		subQuery = subQuery.Where("user_id = ?", userId)
	}
	subQuery.Count(&count)
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

func NewStatistic(data *data.Data) *StatisticService {
	return &StatisticService{
		UnimplementedStatisticServer: statistic.UnimplementedStatisticServer{},
		db:                           data.DB,
		rdb:                          data.RDB,
	}
}
