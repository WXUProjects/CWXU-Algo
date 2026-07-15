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

// resolveMembers 当前组织成员；siteWide=true 且站管时返回 nil（全站不限制）
func (uc *StatisticUseCase) resolveMembers(ctx context.Context, siteWide bool) (memberIDs []int64) {
	if siteWide && auth.VerifySiteAdmin(ctx) {
		return nil
	}
	ids, _, _, err := fetchOrgMemberIDs(ctx, uc.reg, 0)
	if err != nil {
		log.Warnf("statistic org members: %v", err)
		return []int64{}
	}
	return ids
}

// userId 约定：个人>0；0=组织热力；-1=组织时段；-2=全站时段/热力（仅站管）
func isSiteWideUserId(userId int64) bool {
	return userId == -2
}

// Heatmap 获取热力图数据
func (uc *StatisticUseCase) Heatmap(ctx context.Context, req *statistic.HeatmapReq) (*statistic.HeatmapResp, error) {
	if req.StartDate == "" || req.EndDate == "" {
		return nil, errors.BadRequest("参数错误", "日期参数错误")
	}

	var memberIDs []int64
	cacheSuffix := ""
	queryUserId := req.UserId
	if req.UserId == 0 || isSiteWideUserId(req.UserId) {
		siteWide := isSiteWideUserId(req.UserId)
		if siteWide && !auth.VerifySiteAdmin(ctx) {
			return nil, errors.Forbidden("权限不足", "仅站点管理员可查看全站统计")
		}
		memberIDs = uc.resolveMembers(ctx, siteWide)
		queryUserId = 0 // 聚合查询
		if siteWide {
			cacheSuffix = ":site"
		} else if pd := auth.GetCurrentUser(ctx); pd != nil {
			cacheSuffix = fmt.Sprintf(":org%d", pd.OrgID)
		} else {
			cacheSuffix = fmt.Sprintf(":m%d", len(memberIDs))
		}
	}

	ver := "0"
	if queryUserId == 0 {
		if v, err := uc.rdb.Get(ctx, "statistic:heatmap:global:ver").Result(); err == nil && v != "" {
			ver = v
		}
	}
	cacheKey := fmt.Sprintf("statistic:heatmap:%d:%s:%s:%t:v%s%s", req.UserId, req.StartDate, req.EndDate, req.IsAc, ver, cacheSuffix)
	result, _, err := data2.GetCacheDal[[]dal.DailyCount](ctx, uc.rdb, cacheKey, func(data *[]dal.DailyCount) error {
		var err error
		*data, err = uc.dal.HeatmapQueryScoped(ctx, req.StartDate, req.EndDate, queryUserId, req.IsAc, memberIDs)
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

	memberIDs := uc.resolveMembers(ctx, false)
	items, total, err := uc.dal.GetRankByRangeScoped(ctx, start, endExclusive, scoreType, groupId, req.Page, req.PageSize, memberIDs)
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}

	uids := make([]int64, 0, len(items))
	for _, v := range items {
		if v.UserID > 0 {
			uids = append(uids, v.UserID)
		}
	}
	nameMap := fetchDisplayNames(ctx, uc.reg, uids)

	data := make([]*statistic.RankItem, len(items))
	for i, v := range items {
		name := nameMap[v.UserID]
		if name == "" {
			name = v.Name
		}
		data[i] = &statistic.RankItem{
			Rank:   v.Rank,
			UserId: v.UserID,
			Name:   name,
			Score:  v.Score,
		}
	}
	return &statistic.RankResp{Code: 0, Data: data, Total: total}, nil
}

// PeriodCount 获取时间段统计数据
func (uc *StatisticUseCase) PeriodCount(ctx context.Context, req *statistic.PeriodCountReq) (*statistic.PeriodCountResp, error) {
	var memberIDs []int64
	queryUserId := req.UserId
	// 个人 period 也带全局版本，避免与组织统计共用脏缓存语义
	// schema v2：Ac 增加 TotalRaw；旧缓存缺字段会反序列化为 0，必须换 key
	const periodCacheSchema = "2"
	ver := "0"
	if v, err := uc.rdb.Get(ctx, "statistic:period:global:ver").Result(); err == nil && v != "" {
		ver = v
	}
	cacheKey := fmt.Sprintf("statistic:period:s%s:%d:v%s", periodCacheSchema, req.UserId, ver)
	if req.UserId == -1 || isSiteWideUserId(req.UserId) {
		siteWide := isSiteWideUserId(req.UserId)
		if siteWide && !auth.VerifySiteAdmin(ctx) {
			return nil, errors.Forbidden("权限不足", "仅站点管理员可查看全站统计")
		}
		memberIDs = uc.resolveMembers(ctx, siteWide)
		queryUserId = -1
		if siteWide {
			cacheKey = fmt.Sprintf("statistic:period:s%s:site:v%s", periodCacheSchema, ver)
		} else if pd := auth.GetCurrentUser(ctx); pd != nil {
			cacheKey = fmt.Sprintf("statistic:period:s%s:org:%d:v%s", periodCacheSchema, pd.OrgID, ver)
		} else {
			cacheKey = fmt.Sprintf("statistic:period:s%s:org:m%d:v%s", periodCacheSchema, len(memberIDs), ver)
		}
	}

	type PeriodCountData struct {
		Submit dal.PeriodSubmitCount
		Ac     dal.PeriodAcCount
	}

	result, _, err := data2.GetCacheDal[PeriodCountData](ctx, uc.rdb, cacheKey, func(data *PeriodCountData) error {
		var err error
		data.Submit, data.Ac, err = uc.dal.GetPeriodCountScoped(queryUserId, memberIDs)
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
				TotalRaw:  result.Ac.TotalRaw,
			},
		},
	}, nil
}
