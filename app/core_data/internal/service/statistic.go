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
	db  *gorm.DB
	rdb *redis.Client
}
type DailyCount struct {
	Day time.Time
	Cnt int64
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
		sub = sub.Where("status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%")
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

func NewStatistic(data *data.Data) *StatisticService {
	return &StatisticService{
		db:  data.DB,
		rdb: data.RDB,
	}
}
