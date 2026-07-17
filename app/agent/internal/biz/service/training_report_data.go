package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"cwxu-algo/api/core/v1/statistic"
	profile2 "cwxu-algo/api/user/v1/profile"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	grpc2 "google.golang.org/grpc"
)

// TrainingReportData 组织/组训练报告聚合数据（规则模板与 AI 共用）
type TrainingReportData struct {
	OrgID            int64       `json:"orgId"`
	GroupID          int64       `json:"groupId"`
	GroupName        string      `json:"groupName,omitempty"`
	ScopeLabel       string      `json:"scopeLabel"`
	StartDate        string      `json:"startDate"`
	EndDate          string      `json:"endDate"`
	PrevStartDate    string      `json:"prevStartDate"`
	PrevEndDate      string      `json:"prevEndDate"`
	MemberCount      int         `json:"memberCount"`
	MemberIDs        []int64     `json:"memberIds"`
	TotalSubmits     int64       `json:"totalSubmits"`
	PrevTotalSubmits int64       `json:"prevTotalSubmits"`
	TotalAC          int64       `json:"totalAc"`
	DailyTrend       []DayCount  `json:"dailyTrend"`
	TopSubmit        []RankEntry `json:"topSubmit"`
	TopAC            []RankEntry `json:"topAc"`
	InactiveMembers  []string    `json:"inactiveMembers"`
	ActiveMembers    int         `json:"activeMembers"`
	// Initiator 发起人（邮件）
	InitiatorUserID int64  `json:"initiatorUserId"`
	InitiatorName   string `json:"initiatorName"`
	InitiatorEmail  string `json:"initiatorEmail"`
}

// LastWeekRange 相对 now 的上一完整周：周一到周日（end=昨天若 now 为周一则上周日）
// 与既有周报一致：weekEnd = 昨天 0 点所在日，weekStart = weekEnd-6
func LastWeekRange(now time.Time) (start, end time.Time) {
	loc := now.Location()
	weekEnd := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, -1)
	weekStart := weekEnd.AddDate(0, 0, -6)
	return weekStart, weekEnd
}

// ParseDateRange 解析 YYYY-MM-DD，含首尾
func ParseDateRange(startS, endS string) (start, end time.Time, err error) {
	start, err = time.ParseInLocation(dateLayout, strings.TrimSpace(startS), time.Local)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("startDate 格式应为 YYYY-MM-DD")
	}
	end, err = time.ParseInLocation(dateLayout, strings.TrimSpace(endS), time.Local)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("endDate 格式应为 YYYY-MM-DD")
	}
	if end.Before(start) {
		return time.Time{}, time.Time{}, fmt.Errorf("endDate 不能早于 startDate")
	}
	if end.Sub(start).Hours()/24 > 400 {
		return time.Time{}, time.Time{}, fmt.Errorf("日期跨度不能超过 400 天")
	}
	return start, end, nil
}

func (uc *SummaryUseCase) dialUserCtx(ctx context.Context) (*grpc2.ClientConn, error) {
	if uc == nil || uc.reg == nil {
		return nil, fmt.Errorf("service discovery 未配置")
	}
	return grpc.DialInsecure(
		ctx,
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery((*uc.reg).(registry.Discovery)),
		grpc.WithTimeout(30*time.Second),
	)
}

func (uc *SummaryUseCase) dialCoreCtx(ctx context.Context) (*grpc2.ClientConn, error) {
	if uc == nil || uc.reg == nil {
		return nil, fmt.Errorf("service discovery 未配置")
	}
	return grpc.DialInsecure(
		ctx,
		grpc.WithEndpoint("discovery:///core-data"),
		grpc.WithDiscovery((*uc.reg).(registry.Discovery)),
		grpc.WithTimeout(30*time.Second),
	)
}

// resolveMemberIDs 组织成员，可选按组过滤
func (uc *SummaryUseCase) resolveMemberIDs(ctx context.Context, orgID, groupID int64) ([]int64, error) {
	conn, err := uc.dialUserCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	cli := profile2.NewProfileClient(conn)

	var ids []int64
	if groupID > 0 {
		res, err := cli.GetUserIdsByGroup(ctx, &profile2.GetUserIdsByGroupReq{GroupId: groupID})
		if err != nil {
			return nil, fmt.Errorf("按组取成员失败: %w", err)
		}
		ids = res.GetUserIds()
		// 再与组织成员求交，避免跨组织组
		if orgID > 0 {
			orgRes, err := cli.GetUserIdsByOrg(ctx, &profile2.GetUserIdsByOrgReq{OrgId: orgID})
			if err == nil && orgRes != nil {
				orgSet := make(map[int64]struct{}, len(orgRes.UserIds))
				for _, id := range orgRes.UserIds {
					orgSet[id] = struct{}{}
				}
				filtered := make([]int64, 0, len(ids))
				for _, id := range ids {
					if _, ok := orgSet[id]; ok {
						filtered = append(filtered, id)
					}
				}
				ids = filtered
			}
		}
	} else {
		res, err := cli.GetUserIdsByOrg(ctx, &profile2.GetUserIdsByOrgReq{OrgId: orgID})
		if err != nil {
			return nil, fmt.Errorf("取组织成员失败: %w", err)
		}
		ids = res.GetUserIds()
	}
	if ids == nil {
		ids = []int64{}
	}
	return ids, nil
}

func (uc *SummaryUseCase) fetchNames(ctx context.Context, userIDs []int64, orgID int64) map[int64]string {
	out := make(map[int64]string, len(userIDs))
	if len(userIDs) == 0 {
		return out
	}
	conn, err := uc.dialUserCtx(ctx)
	if err != nil {
		return out
	}
	defer conn.Close()
	cli := profile2.NewProfileClient(conn)
	const batch = 100
	for i := 0; i < len(userIDs); i += batch {
		j := i + batch
		if j > len(userIDs) {
			j = len(userIDs)
		}
		res, err := cli.GetByIds(ctx, &profile2.GetByIdsReq{UserIds: userIDs[i:j], OrgId: orgID})
		if err != nil || res == nil {
			continue
		}
		for _, p := range res.Profiles {
			if p.Name != "" {
				out[p.UserId] = p.Name
			} else if p.Username != "" {
				out[p.UserId] = p.Username
			}
		}
	}
	return out
}

func (uc *SummaryUseCase) fetchHeatmapUser(ctx context.Context, userId int64, start, end time.Time, isAC bool) ([]DayCount, error) {
	conn, err := uc.dialCoreCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	cli := statistic.NewStatisticClient(conn)
	res, err := cli.Heatmap(ctx, &statistic.HeatmapReq{
		UserId:    userId,
		StartDate: start.Format(dateLayout),
		EndDate:   end.Format(dateLayout),
		IsAc:      isAC,
	})
	if err != nil {
		return nil, err
	}
	var items []*statistic.HeatmapResp_HeatmapItem
	if res != nil {
		items = res.Data
	}
	return fillMissingDays(start, end, items), nil
}

// LoadTrainingReportData 拉取组织/组在日期区间的训练数据
func (uc *SummaryUseCase) LoadTrainingReportData(ctx context.Context, orgID, groupID, initiatorID int64, start, end time.Time) (*TrainingReportData, error) {
	if orgID <= 0 {
		return nil, fmt.Errorf("缺少组织 id")
	}
	memberIDs, err := uc.resolveMemberIDs(ctx, orgID, groupID)
	if err != nil {
		return nil, err
	}

	days := int(end.Sub(start).Hours()/24) + 1
	prevEnd := start.AddDate(0, 0, -1)
	prevStart := prevEnd.AddDate(0, 0, -(days - 1))

	// 聚合每日提交
	dayTotals := make([]DayCount, 0, days)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		dayTotals = append(dayTotals, DayCount{Date: d.Format(dateLayout), Count: 0})
	}
	submitByUser := make(map[int64]int64, len(memberIDs))
	acByUser := make(map[int64]int64, len(memberIDs))
	var prevTotal int64

	// 分批拉个人热力（提交 + AC）
	for _, uid := range memberIDs {
		hm, err := uc.fetchHeatmapUser(ctx, uid, start, end, false)
		if err != nil {
			log.Warnf("training report heatmap user=%d: %v", uid, err)
			continue
		}
		var uSum int64
		for i, d := range hm {
			if i < len(dayTotals) {
				dayTotals[i].Count += d.Count
			}
			uSum += d.Count
		}
		submitByUser[uid] = uSum

		acHm, err := uc.fetchHeatmapUser(ctx, uid, start, end, true)
		if err == nil {
			var acSum int64
			for _, d := range acHm {
				acSum += d.Count
			}
			acByUser[uid] = acSum
		}

		prevHm, err := uc.fetchHeatmapUser(ctx, uid, prevStart, prevEnd, false)
		if err == nil {
			prevTotal += sumDayCounts(prevHm)
		}
	}

	nameMap := uc.fetchNames(ctx, memberIDs, orgID)

	topSubmit := rankFromMap(submitByUser, nameMap, 10)
	topAC := rankFromMap(acByUser, nameMap, 10)

	// 不活跃：区间内 0 提交，或最后提交早于 end-2 天
	threshold := end.AddDate(0, 0, -2)
	lastMap, err := uc.fetchLastSubmitMap(ctx, memberIDs)
	if err != nil {
		log.Warnf("training report lastSubmit: %v", err)
		lastMap = map[int64]int64{}
	}
	inactive := make([]string, 0)
	active := 0
	type pair struct {
		id   int64
		name string
		ts   int64
	}
	cands := make([]pair, 0)
	for _, id := range memberIDs {
		if submitByUser[id] > 0 {
			active++
			continue
		}
		ts := lastMap[id]
		if ts > 0 && !time.Unix(ts, 0).Before(threshold) {
			// 区间外最近仍活跃但本区间 0 —— 仍算本区间不活跃
		}
		n := nameMap[id]
		if n == "" {
			n = fmt.Sprintf("用户%d", id)
		}
		cands = append(cands, pair{id: id, name: n, ts: ts})
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].ts < cands[j].ts })
	for _, c := range cands {
		inactive = append(inactive, c.name)
		if len(inactive) >= 50 {
			break
		}
	}

	scopeLabel := "整组织"
	if groupID > 0 {
		scopeLabel = fmt.Sprintf("组#%d", groupID)
	}

	initName := ""
	initEmail := ""
	if initiatorID > 0 {
		initEmail = uc.userContactEmail(initiatorID)
		if p := uc.userProfile(initiatorID); p != nil {
			initName = p.Name
		}
		if initName == "" {
			initName = fmt.Sprintf("用户%d", initiatorID)
		}
	}

	return &TrainingReportData{
		OrgID:            orgID,
		GroupID:          groupID,
		ScopeLabel:       scopeLabel,
		StartDate:        start.Format(dateLayout),
		EndDate:          end.Format(dateLayout),
		PrevStartDate:    prevStart.Format(dateLayout),
		PrevEndDate:      prevEnd.Format(dateLayout),
		MemberCount:      len(memberIDs),
		MemberIDs:        memberIDs,
		TotalSubmits:     sumDayCounts(dayTotals),
		PrevTotalSubmits: prevTotal,
		TotalAC:          sumMap(acByUser),
		DailyTrend:       dayTotals,
		TopSubmit:        topSubmit,
		TopAC:            topAC,
		InactiveMembers:  inactive,
		ActiveMembers:    active,
		InitiatorUserID:  initiatorID,
		InitiatorName:    initName,
		InitiatorEmail:   initEmail,
	}, nil
}

func sumMap(m map[int64]int64) int64 {
	var s int64
	for _, v := range m {
		s += v
	}
	return s
}

func rankFromMap(scores map[int64]int64, names map[int64]string, topN int) []RankEntry {
	type pair struct {
		id    int64
		score int64
	}
	arr := make([]pair, 0, len(scores))
	for id, sc := range scores {
		if sc <= 0 {
			continue
		}
		arr = append(arr, pair{id: id, score: sc})
	}
	sort.Slice(arr, func(i, j int) bool {
		if arr[i].score == arr[j].score {
			return arr[i].id < arr[j].id
		}
		return arr[i].score > arr[j].score
	})
	if topN > 0 && len(arr) > topN {
		arr = arr[:topN]
	}
	out := make([]RankEntry, 0, len(arr))
	for i, p := range arr {
		n := names[p.id]
		if n == "" {
			n = fmt.Sprintf("用户%d", p.id)
		}
		out = append(out, RankEntry{
			Rank:   int64(i + 1),
			UserID: p.id,
			Name:   n,
			Score:  p.score,
		})
	}
	return out
}
