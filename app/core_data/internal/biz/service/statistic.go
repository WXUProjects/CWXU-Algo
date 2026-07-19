package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cwxu-algo/api/core/v1/statistic"
	data2 "cwxu-algo/app/common/data"
	"cwxu-algo/app/common/discovery"
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

// resolveMembers 当前组织成员；siteWide=true 时返回 nil（全站不限制，公开聚合）
func (uc *StatisticUseCase) resolveMembers(ctx context.Context, siteWide bool) (memberIDs []int64, orgID uint) {
	if siteWide {
		return nil, 0
	}
	ids, resolvedOrgID, _, err := fetchOrgMemberIDs(ctx, uc.reg, 0)
	if err != nil {
		log.Warnf("statistic org members: %v", err)
		return []int64{}, resolvedOrgID
	}
	return ids, resolvedOrgID
}

// userId 约定：个人>0；0=组织热力；-1=组织时段；-2=全站时段/热力（公开聚合，无需站管）
func isSiteWideUserId(userId int64) bool {
	return userId == -2
}

func (uc *StatisticUseCase) redisVer(ctx context.Context, key string) string {
	if v, err := uc.rdb.Get(ctx, key).Result(); err == nil && v != "" {
		return v
	}
	return "0"
}

// heatmapMaxDays 2c4g / 1w 日活：热力最大跨度（约 13 个月），防止 2023→今 的超长 series
const heatmapMaxDays = 400

// clampHeatmapRange 规范化并限制日期跨度；入参支持 20060102 / 2006-01-02
func clampHeatmapRange(startS, endS string) (start, end string, err error) {
	parse := func(s string) (time.Time, error) {
		if t, e := time.ParseInLocation("2006-01-02", s, time.Local); e == nil {
			return t, nil
		}
		return time.ParseInLocation("20060102", s, time.Local)
	}
	st, e1 := parse(startS)
	en, e2 := parse(endS)
	if e1 != nil || e2 != nil {
		return "", "", errors.BadRequest("参数错误", "日期格式错误")
	}
	if en.Before(st) {
		st, en = en, st
	}
	if int(en.Sub(st).Hours()/24) > heatmapMaxDays {
		st = en.AddDate(0, 0, -heatmapMaxDays)
	}
	return st.Format("2006-01-02"), en.Format("2006-01-02"), nil
}

// Heatmap 获取热力图数据
func (uc *StatisticUseCase) Heatmap(ctx context.Context, req *statistic.HeatmapReq) (*statistic.HeatmapResp, error) {
	if req.StartDate == "" || req.EndDate == "" {
		return nil, errors.BadRequest("参数错误", "日期参数错误")
	}
	startDate, endDate, err := clampHeatmapRange(req.StartDate, req.EndDate)
	if err != nil {
		return nil, err
	}

	var memberIDs []int64
	queryUserId := req.UserId
	ttl := data2.DefaultCacheTTL
	var cacheKey string

	if req.UserId > 0 {
		// 个人：用用户级 ver，其它用户爬虫不会失效本缓存
		userVer := uc.redisVer(ctx, fmt.Sprintf("statistic:user:%d:ver", req.UserId))
		cacheKey = fmt.Sprintf("statistic:heatmap:u%d:%s:%s:%t:v%s", req.UserId, startDate, endDate, req.IsAc, userVer)
	} else if req.UserId == 0 || isSiteWideUserId(req.UserId) {
		// -2=全站公开聚合；0=当前组织热力
		siteWide := isSiteWideUserId(req.UserId)
		var resolvedOrgID uint
		memberIDs, resolvedOrgID = uc.resolveMembers(ctx, siteWide)
		queryUserId = 0 // 聚合查询
		globalVer := uc.redisVer(ctx, "statistic:heatmap:global:ver")
		ttl = data2.OrgStatsCacheTTL
		if siteWide {
			cacheKey = fmt.Sprintf("statistic:heatmap:site:%s:%s:%t:v%s", startDate, endDate, req.IsAc, globalVer)
		} else {
			cacheKey = fmt.Sprintf("statistic:heatmap:org%d:%s:%s:%t:v%s", resolvedOrgID, startDate, endDate, req.IsAc, globalVer)
		}
	} else {
		// 兼容其它 userId（如历史 -1 等）：走全局 ver
		globalVer := uc.redisVer(ctx, "statistic:heatmap:global:ver")
		cacheKey = fmt.Sprintf("statistic:heatmap:%d:%s:%s:%t:v%s", req.UserId, startDate, endDate, req.IsAc, globalVer)
	}

	result, _, err := data2.GetCacheDalTTL[[]dal.DailyCount](ctx, uc.rdb, cacheKey, ttl, func(data *[]dal.DailyCount) error {
		var err error
		*data, err = uc.dal.HeatmapQueryScoped(ctx, startDate, endDate, queryUserId, req.IsAc, memberIDs)
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

	memberIDs, orgID := uc.resolveMembers(ctx, false)
	// 公共域 / 未登录回落公共域：全站聚合（nil），避免对全体成员做巨型 IN，
	// 也避免 resolve 失败时空列表导致「无法加载」。私有域仍按成员隔离。
	if isPublicOrgID(ctx, uc.reg, orgID) {
		memberIDs = nil
	}

	// TopN 首页快照：page=1、无 group、pageSize≤50
	useSnap := req.Page <= 1 && groupId < 0 && req.PageSize > 0 && req.PageSize <= 50 && uc.rdb != nil
	globalVer := uc.redisVer(ctx, "statistic:period:global:ver")
	snapKey := ""
	if useSnap {
		scope := "org"
		if memberIDs == nil {
			scope = "all"
		} else if orgID == 0 {
			scope = "all"
		} else {
			scope = fmt.Sprintf("org%d", orgID)
		}
		// schema s3：生涯过题力扣优先官方 acTotal 合成键
		snapKey = fmt.Sprintf("rank:snap:s3:%s:%s:%s_%s:v%s:ps%d",
			scope, scoreType, req.StartDate, req.EndDate, globalVer, req.PageSize)
		if b, e := uc.rdb.Get(ctx, snapKey).Bytes(); e == nil && len(b) > 0 {
			var cached statistic.RankResp
			if json.Unmarshal(b, &cached) == nil && cached.Data != nil {
				cached.Code = 0
				return &cached, nil
			}
		}
	}

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
	resp := &statistic.RankResp{Code: 0, Data: data, Total: total}
	if useSnap && snapKey != "" {
		if b, e := json.Marshal(resp); e == nil {
			_ = uc.rdb.Set(ctx, snapKey, b, data2.OrgStatsCacheTTL).Err()
		}
	}
	return resp, nil
}

// PeriodCount 获取时间段统计数据
func (uc *StatisticUseCase) PeriodCount(ctx context.Context, req *statistic.PeriodCountReq) (*statistic.PeriodCountResp, error) {
	var memberIDs []int64
	queryUserId := req.UserId
	// schema v6：个人 AC 去重走 user_ac_problem_* 预聚合
	// s9：生涯 total 力扣优先官方 acTotal 合成键（与平台过题一致，避免 recentAC 双计）
	const periodCacheSchema = "9"
	ttl := data2.DefaultCacheTTL
	var cacheKey string

	if req.UserId > 0 {
		userVer := uc.redisVer(ctx, fmt.Sprintf("statistic:user:%d:ver", req.UserId))
		cacheKey = fmt.Sprintf("statistic:period:s%s:u%d:v%s", periodCacheSchema, req.UserId, userVer)
		// 热度：供爬虫后自主预热决策
		TouchUserHeat(ctx, uc.rdb, req.UserId)
	} else if req.UserId == -1 || isSiteWideUserId(req.UserId) {
		// -2=全站公开聚合；-1=当前组织时段
		siteWide := isSiteWideUserId(req.UserId)
		var resolvedOrgID uint
		memberIDs, resolvedOrgID = uc.resolveMembers(ctx, siteWide)
		queryUserId = -1
		globalVer := uc.redisVer(ctx, "statistic:period:global:ver")
		ttl = data2.OrgStatsCacheTTL
		if siteWide {
			cacheKey = fmt.Sprintf("statistic:period:s%s:site:v%s", periodCacheSchema, globalVer)
		} else {
			cacheKey = fmt.Sprintf("statistic:period:s%s:org:%d:v%s", periodCacheSchema, resolvedOrgID, globalVer)
		}
	} else {
		// 兼容 0 等：按全局
		globalVer := uc.redisVer(ctx, "statistic:period:global:ver")
		cacheKey = fmt.Sprintf("statistic:period:s%s:%d:v%s", periodCacheSchema, req.UserId, globalVer)
	}

	type PeriodCountData struct {
		Submit dal.PeriodSubmitCount
		Ac     dal.PeriodAcCount
	}

	result, _, err := data2.GetCacheDalTTL[PeriodCountData](ctx, uc.rdb, cacheKey, ttl, func(data *PeriodCountData) error {
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
