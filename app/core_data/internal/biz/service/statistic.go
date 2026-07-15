package service

import (
	"context"
	"fmt"
	"time"

	"cwxu-algo/api/core/v1/statistic"
	data2 "cwxu-algo/app/common/data"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/core_data/internal/data/dal"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/redis/go-redis/v9"
)

// StatisticUseCase 统计业务逻辑层
type StatisticUseCase struct {
	dal *dal.StatisticDal
	rdb *redis.Client
	reg *registry.Registrar
}

// NewStatisticUseCase 创建统计业务逻辑层
func NewStatisticUseCase(dal *dal.StatisticDal, rdb *redis.Client, reg *discovery.Register) *StatisticUseCase {
	var r *registry.Registrar
	if reg != nil {
		r = &reg.Reg
	}
	return &StatisticUseCase{dal: dal, rdb: rdb, reg: r}
}

func (uc *StatisticUseCase) resolveMembers(ctx context.Context) (memberIDs []int64) {
	ids, _, _, err := fetchOrgMemberIDs(ctx, uc.reg, 0)
	if err != nil {
		log.Warnf("statistic org members: %v", err)
		return []int64{}
	}
	return ids
}

// Heatmap 获取热力图数据
func (uc *StatisticUseCase) Heatmap(ctx context.Context, req *statistic.HeatmapReq) (*statistic.HeatmapResp, error) {
	if req.StartDate == "" || req.EndDate == "" {
		return nil, errors.BadRequest("参数错误", "日期参数错误")
	}

	var memberIDs []int64
	cacheSuffix := ""
	if req.UserId == 0 {
		memberIDs = uc.resolveMembers(ctx)
		if pd := auth.GetCurrentUser(ctx); pd != nil {
			cacheSuffix = fmt.Sprintf(":org%d", pd.OrgID)
		} else {
			cacheSuffix = fmt.Sprintf(":m%d", len(memberIDs))
		}
	}

	ver := "0"
	if req.UserId == 0 {
		if v, err := uc.rdb.Get(ctx, "statistic:heatmap:global:ver").Result(); err == nil && v != "" {
			ver = v
		}
	}
	cacheKey := fmt.Sprintf("statistic:heatmap:%d:%s:%s:%t:v%s%s", req.UserId, req.StartDate, req.EndDate, req.IsAc, ver, cacheSuffix)
	result, _, err := data2.GetCacheDal[[]dal.DailyCount](ctx, uc.rdb, cacheKey, func(data *[]dal.DailyCount) error {
		var err error
		*data, err = uc.dal.HeatmapQueryScoped(ctx, req.StartDate, req.EndDate, req.UserId, req.IsAc, memberIDs)
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

	return &statistic.HeatmapResp{Code: 0, Data: items}, nil
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
	endExclusive := end.AddDate(0, 0, 1)

	scoreType := req.ScoreType
	if scoreType == "" {
		scoreType = "submit"
	}
	groupId := req.GroupId
	if groupId == 0 {
		groupId = -1
	}

	memberIDs := uc.resolveMembers(ctx)
	items, total, err := uc.dal.GetRankByRangeScoped(ctx, start, endExclusive, scoreType, groupId, req.Page, req.PageSize, memberIDs)
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
	return &statistic.RankResp{Code: 0, Data: data, Total: total}, nil
}

// PeriodCount 获取时间段统计数据
func (uc *StatisticUseCase) PeriodCount(ctx context.Context, req *statistic.PeriodCountReq) (*statistic.PeriodCountResp, error) {
	var memberIDs []int64
	cacheKey := fmt.Sprintf("statistic:period:%d", req.UserId)
	if req.UserId == -1 {
		memberIDs = uc.resolveMembers(ctx)
		if pd := auth.GetCurrentUser(ctx); pd != nil {
			cacheKey = fmt.Sprintf("statistic:period:org:%d", pd.OrgID)
		} else {
			cacheKey = fmt.Sprintf("statistic:period:org:m%d", len(memberIDs))
		}
	}

	type PeriodCountData struct {
		Submit dal.PeriodSubmitCount
		Ac     dal.PeriodAcCount
	}

	result, _, err := data2.GetCacheDal[PeriodCountData](ctx, uc.rdb, cacheKey, func(data *PeriodCountData) error {
		var err error
		data.Submit, data.Ac, err = uc.dal.GetPeriodCountScoped(req.UserId, memberIDs)
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
