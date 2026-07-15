package service

import (
	"context"
	"fmt"
	"time"

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

	// 全局热力图带版本号，爬虫只 INCR 版本即可失效，无需 SCAN 全库
	ver := "0"
	if req.UserId == 0 {
		if v, err := uc.rdb.Get(ctx, "statistic:heatmap:global:ver").Result(); err == nil && v != "" {
			ver = v
		}
	}
	cacheKey := fmt.Sprintf("statistic:heatmap:%d:%s:%s:%t:v%s", req.UserId, req.StartDate, req.EndDate, req.IsAc, ver)
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

// Rank 按日期区间获取排行
func (uc *StatisticUseCase) Rank(ctx context.Context, req *statistic.RankReq) (*statistic.RankResp, error) {
	if req.StartDate == "" || req.EndDate == "" {
		return nil, errors.BadRequest("参数错误", "日期参数错误")
	}
	start, err := time.ParseInLocation("2006-01-02", req.StartDate, time.Local)
	if err != nil {
		return nil, errors.BadRequest("参数错误", "startDate 格式应为 YYYY-MM-DD")
	}
	end, err := time.ParseInLocation("2006-01-02", req.EndDate, time.Local)
	if err != nil {
		return nil, errors.BadRequest("参数错误", "endDate 格式应为 YYYY-MM-DD")
	}
	// endDate 含当天，查询用次日 0 点开区间
	endExclusive := end.AddDate(0, 0, 1)

	scoreType := req.ScoreType
	if scoreType == "" {
		scoreType = "submit"
	}
	groupId := req.GroupId
	if groupId == 0 {
		groupId = -1
	}
	page := req.Page
	pageSize := req.PageSize

	items, total, err := uc.dal.GetRankByRange(ctx, start, endExclusive, scoreType, groupId, page, pageSize)
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}

	data := make([]*statistic.RankItem, len(items))
	for i, v := range items {
		data[i] = &statistic.RankItem{
			Rank:   v.Rank,
			UserId: v.UserID,
			Name:   v.Name,
			Score:  v.Score,
		}
	}
	return &statistic.RankResp{
		Code:  0,
		Data:  data,
		Total: total,
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
