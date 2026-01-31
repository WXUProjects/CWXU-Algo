package service

import (
	"context"
	"fmt"

	"cwxu-algo/api/core/v1/statistic"
	data2 "cwxu-algo/app/common/data"
	"cwxu-algo/app/core_data/internal/data/dal"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/redis/go-redis/v9"
)

// StatisticUseCase 统计业务逻辑层
type StatisticUseCase struct {
	dal *dal.StatisticDal
	rdb *redis.Client
}

// NewStatisticUseCase 创建统计业务逻辑层
func NewStatisticUseCase(dal *dal.StatisticDal, rdb *redis.Client) *StatisticUseCase {
	return &StatisticUseCase{
		dal: dal,
		rdb: rdb,
	}
}

// Heatmap 获取热力图数据
// 参数来自 statistic.proto:
// - userId: 用户ID，0表示所有用户
// - startDate: 开始日期 (YYYY-MM-DD)
// - endDate: 结束日期 (YYYY-MM-DD)
// - isAc: 是否只统计AC提交
func (uc *StatisticUseCase) Heatmap(ctx context.Context, req *statistic.HeatmapReq) (*statistic.HeatmapResp, error) {
	if req.StartDate == "" || req.EndDate == "" {
		return nil, errors.BadRequest("参数错误", "日期参数错误")
	}

	cacheKey := fmt.Sprintf("statistic:heatmap:%d:%s:%s:%t", req.UserId, req.StartDate, req.EndDate, req.IsAc)
	result, _, err := data2.GetCacheDal[[]dal.DailyCount](ctx, uc.rdb, cacheKey, func(data *[]dal.DailyCount) error {
		var err error
		*data, err = uc.dal.HeatmapQuery(ctx, req.StartDate, req.EndDate, req.UserId, req.IsAc)
		return err
	})
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}

	items := make([]*statistic.HeatmapResp_HeatmapItem, len(*result))
	for i, v := range *result {
		items[i] = &statistic.HeatmapResp_HeatmapItem{
			Date:  v.Day.Format("2006-01-02"),
			Count: v.Cnt,
		}
	}

	return &statistic.HeatmapResp{
		Code: 0,
		Data: items,
	}, nil
}

// PeriodCount 获取时间段统计数据
// 参数来自 statistic.proto:
// - userId: 用户ID，-1表示所有用户
func (uc *StatisticUseCase) PeriodCount(ctx context.Context, req *statistic.PeriodCountReq) (*statistic.PeriodCountResp, error) {
	cacheKey := fmt.Sprintf("statistic:period:%d", req.UserId)

	type PeriodCountData struct {
		Submit dal.PeriodSubmitCount
		Ac     dal.PeriodAcCount
	}

	result, _, err := data2.GetCacheDal[PeriodCountData](ctx, uc.rdb, cacheKey, func(data *PeriodCountData) error {
		var err error
		data.Submit, data.Ac, err = uc.dal.GetPeriodCount(req.UserId)
		return err
	})
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}

	return &statistic.PeriodCountResp{
		Code: 0,
		Data: &statistic.PeriodData{
			Submit: &statistic.SubmitCount{
				Today:     result.Submit.Today,
				ThisWeek:  result.Submit.ThisWeek,
				LastWeek:  result.Submit.LastWeek,
				ThisMonth: result.Submit.ThisMonth,
				LastMonth: result.Submit.LastMonth,
				ThisYear:  result.Submit.ThisYear,
				LastYear:  result.Submit.LastYear,
				Total:     result.Submit.Total,
			},
			Ac: &statistic.AcCount{
				Today:     result.Ac.Today,
				ThisWeek:  result.Ac.ThisWeek,
				LastWeek:  result.Ac.LastWeek,
				ThisMonth: result.Ac.ThisMonth,
				LastMonth: result.Ac.LastMonth,
				ThisYear:  result.Ac.ThisYear,
				LastYear:  result.Ac.LastYear,
				Total:     result.Ac.Total,
			},
		},
	}, nil
}

// Rank 获取排行榜数据
// 参数来自 statistic.proto:
// - userId: 用户ID，0表示所有用户
// - time: 时间维度 (日/周/月)
// - scoreType: 分数类型 ("ac"表示AC排行榜，其他表示提交排行榜)
// - groupId: 分组ID，-1表示所有分组
// - page: 页码
// - pageSize: 每页大小
func (uc *StatisticUseCase) Rank(ctx context.Context, req *statistic.RankReq) (*statistic.RankResp, error) {
	cacheKey := fmt.Sprintf("statistic:rank:%d:%s:%s:%d:%d:%d", req.UserId, req.Time, req.ScoreType, req.GroupId, req.Page, req.PageSize)

	type RankCacheData struct {
		Items []dal.RankItem
		Total int64
	}

	result, _, err := data2.GetCacheDal[RankCacheData](ctx, uc.rdb, cacheKey, func(data *RankCacheData) error {
		var err error
		data.Items, data.Total, err = uc.dal.GetRank(ctx, req.UserId, req.Time, req.ScoreType, req.GroupId, req.Page, req.PageSize)
		return err
	})
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}

	items := make([]*statistic.RankItem, len(result.Items))
	for i, v := range result.Items {
		items[i] = &statistic.RankItem{
			Rank:   v.Rank,
			UserId: v.UserID,
			Name:   v.Name,
			Score:  v.Score,
		}
	}

	return &statistic.RankResp{
		Code: 0,
		Data: items,
	}, nil
}