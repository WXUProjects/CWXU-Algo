package service

import (
	"context"
	"fmt"
	"time"

	"cwxu-algo/api/core/v1/contest_log"
	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/common/utils"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/dal"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/userrpc"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/redis/go-redis/v9"
	grpc2 "google.golang.org/grpc"
	"gorm.io/gorm"
)

type ContestLogService struct {
	contest_log.UnimplementedContestServer
	sbDal *dal.SpiderDal
	db    *gorm.DB
	rdb   *redis.Client
	reg   *registry.Registrar
}

func (c ContestLogService) userRPC() (*grpc2.ClientConn, error) {
	return userrpc.Conn(c.reg)
}

func (c ContestLogService) GetContestList(ctx context.Context, req *contest_log.GetContestListReq) (*contest_log.GetContestListRes, error) {
	var memberIDs []int64
	var resolvedOrg uint
	if req.UserId == -1 {
		ids, orgID, unrestricted, err := ResolveOrgMemberIDs(ctx, c.reg, 0, false)
		if err != nil {
			log.Warnf("org members for contest list: %v", err)
			ids = []int64{}
		}
		resolvedOrg = orgID
		if !unrestricted {
			memberIDs = ids
		}
	}

	type listPayload struct {
		Logs  []model.ContestLog
		Total int64
	}
	var logs []model.ContestLog
	var total int64
	var err error

	// 组织首页短缓存（90s + global ver）
	if req.UserId == -1 && c.rdb != nil && req.Offset == 0 && req.Limit > 0 && req.Limit <= 50 {
		ver := "0"
		if v, e := c.rdb.Get(ctx, "core:contest:list:global:ver").Result(); e == nil && v != "" {
			ver = v
		}
		key := fmt.Sprintf("core:contest:list:org%d:p%s:off%d:lim%d:v%s",
			resolvedOrg, req.Platform, req.Offset, req.Limit, ver)
		if b, e := c.rdb.Get(ctx, key).Bytes(); e == nil && len(b) > 0 {
			var p listPayload
			if utils.GobDecoder(b, &p) == nil {
				logs, total = p.Logs, p.Total
			}
		}
		if logs == nil {
			logs, total, err = c.sbDal.GetContestListScoped(ctx, req.UserId, req.Offset, req.Limit, req.Platform, memberIDs)
			if err != nil {
				return nil, errors.InternalServer("内部服务器错误", "服务暂时不可用")
			}
			if b, e := utils.GobEncoder(listPayload{Logs: logs, Total: total}); e == nil {
				_ = c.rdb.Set(ctx, key, b, 90*time.Second).Err()
			}
		}
	} else {
		logs, total, err = c.sbDal.GetContestListScoped(ctx, req.UserId, req.Offset, req.Limit, req.Platform, memberIDs)
		if err != nil {
			return nil, errors.InternalServer("内部服务器错误", "服务暂时不可用")
		}
	}

	items := make([]*contest_log.ContestLog, 0, len(logs))
	for _, v := range logs {
		items = append(items, &contest_log.ContestLog{
			Id:          uint32(v.ID),
			Platform:    v.Platform,
			ContestId:   v.ContestId,
			ContestName: v.ContestName,
			ContestUrl:  v.ContestUrl,
			TotalCount:  int32(v.TotalCount),
			Time:        v.Time.Unix(),
		})
	}

	return &contest_log.GetContestListRes{
		Code:    0,
		Message: "OK",
		Data:    items,
		Total:   total,
	}, nil
}

func (c ContestLogService) GetContestRanking(ctx context.Context, req *contest_log.GetContestRankingReq) (*contest_log.GetContestRankingRes, error) {
	contest := model.ContestLog{}
	_ = c.db.Where("id = ?", req.ContestId).First(&contest)

	contestProto := &contest_log.ContestLog{
		Id:          uint32(contest.ID),
		Platform:    contest.Platform,
		ContestId:   contest.ContestId,
		ContestName: contest.ContestName,
		ContestUrl:  contest.ContestUrl,
		TotalCount:  int32(contest.TotalCount),
		Time:        contest.Time.Unix(),
	}

	// 复用进程内 user 长连接
	var userClient profile.ProfileClient
	if cli, err := userrpc.ProfileClient(c.reg); err != nil {
		log.Errorf("userRPC failed: %v", err)
	} else {
		userClient = cli
	}

	var userIds []int64
	if req.GroupId != nil && userClient != nil {
		res, err := userClient.GetUserIdsByGroup(ctx, &profile.GetUserIdsByGroupReq{GroupId: *req.GroupId})
		if err != nil {
			log.Errorf("GetUserIdsByGroup failed: %v", err)
			return nil, errors.InternalServer("内部服务器错误", "获取用户组信息失败")
		}
		userIds = res.UserIds
		if len(userIds) == 0 {
			return &contest_log.GetContestRankingRes{
				Code:    0,
				Message: "OK",
				Contest: contestProto,
				Data:    make([]*contest_log.RankingItem, 0),
				Total:   0,
			}, nil
		}
	} else if userClient != nil {
		// 默认队内榜：当前组织成员
		ids, _, unrestricted, err := ResolveOrgMemberIDsFromConn(ctx, userClient, 0, false)
		if err != nil {
			log.Warnf("org members for ranking: %v", err)
			ids = []int64{}
		}
		if !unrestricted {
			userIds = ids
			if len(userIds) == 0 {
				return &contest_log.GetContestRankingRes{
					Code:    0,
					Message: "OK",
					Contest: contestProto,
					Data:    make([]*contest_log.RankingItem, 0),
					Total:   0,
				}, nil
			}
		}
	}

	// 只看关注：与当前域/分组成员求交（仍受域限制）
	if req.FollowingOnly {
		viewer := auth.GetCurrentUserId(ctx)
		if viewer == 0 {
			return &contest_log.GetContestRankingRes{
				Code:    0,
				Message: "OK",
				Contest: contestProto,
				Data:    make([]*contest_log.RankingItem, 0),
				Total:   0,
			}, nil
		}
		following := fetchFollowingIDs(ctx, c.reg, int64(viewer))
		if userIds == nil {
			// unrestricted 全站站管路径：仅关注
			userIds = following
		} else {
			userIds = intersectIDs(userIds, following)
		}
		if len(userIds) == 0 {
			return &contest_log.GetContestRankingRes{
				Code:    0,
				Message: "OK",
				Contest: contestProto,
				Data:    make([]*contest_log.RankingItem, 0),
				Total:   0,
			}, nil
		}
	}

	// 非 following 的榜单短缓存（60s）；scope 用 group 或成员数量哈希
	type rankPayload struct {
		Logs  []model.ContestLog
		Total int64
	}
	var logs []model.ContestLog
	var total int64
	var err error
	rankCacheKey := ""
	if !req.FollowingOnly && c.rdb != nil && req.Offset == 0 && req.Limit > 0 && req.Limit <= 50 {
		scope := "all"
		if req.GroupId != nil {
			scope = fmt.Sprintf("g%d", *req.GroupId)
		} else if userIds != nil {
			scope = fmt.Sprintf("n%d", len(userIds))
		}
		rankCacheKey = fmt.Sprintf("core:contest:rank:%s:%s:%s:off%d:lim%d",
			contest.Platform, contest.ContestId, scope, req.Offset, req.Limit)
		if b, e := c.rdb.Get(ctx, rankCacheKey).Bytes(); e == nil && len(b) > 0 {
			var p rankPayload
			if utils.GobDecoder(b, &p) == nil {
				logs, total = p.Logs, p.Total
			}
		}
	}
	if logs == nil {
		logs, total, err = c.sbDal.GetContestRanking(ctx, contest.ContestId, contest.Platform, req.Offset, req.Limit, userIds)
		if err != nil {
			return nil, errors.InternalServer("内部服务器错误", "服务暂时不可用")
		}
		if rankCacheKey != "" {
			if b, e := utils.GobEncoder(rankPayload{Logs: logs, Total: total}); e == nil {
				_ = c.rdb.Set(ctx, rankCacheKey, b, 60*time.Second).Err()
			}
		}
	}

	// 批量获取用户信息，一次 RPC 替代原来的 N 次 GetById
	nameMap := c.fetchUserNames(ctx, userClient, logs)

	// 站内榜：有官方 rank 用官方；整页全是 0（未出分/爬失败）则按 AC 排序后模拟 1..n
	allZeroRank := true
	for _, v := range logs {
		if v.Rank > 0 {
			allZeroRank = false
			break
		}
	}

	items := make([]*contest_log.RankingItem, 0, len(logs))
	for i, v := range logs {
		u := nameMap[v.UserID]
		rank := int64(v.Rank)
		if rank <= 0 && allZeroRank {
			rank = req.Offset + int64(i) + 1
		}
		items = append(items, &contest_log.RankingItem{
			Rank:       rank,
			UserId:     v.UserID,
			Name:       u.Name,
			Avatar:     u.Avatar,
			AcCount:    int32(v.AcCount),
			TotalCount: int32(v.TotalCount),
		})
	}

	return &contest_log.GetContestRankingRes{
		Code:    0,
		Message: "OK",
		Contest: contestProto,
		Data:    items,
		Total:   total,
	}, nil
}

type userInfo struct {
	Avatar string
	Name   string
}

// fetchUserNames 批量获取用户姓名和头像，一次 RPC 调用
func (c ContestLogService) fetchUserNames(ctx context.Context, client profile.ProfileClient, logs []model.ContestLog) map[int64]userInfo {
	result := map[int64]userInfo{}
	if client == nil || len(logs) == 0 {
		return result
	}

	// 去重收集 userId
	idSet := map[int64]struct{}{}
	for _, v := range logs {
		if v.UserID != 0 {
			idSet[v.UserID] = struct{}{}
		}
	}
	userIds := make([]int64, 0, len(idSet))
	for id := range idSet {
		userIds = append(userIds, id)
	}

	var orgID int64
	if pd := auth.GetCurrentUser(ctx); pd != nil {
		orgID = int64(pd.OrgID)
	}
	res, err := client.GetByIds(ctx, &profile.GetByIdsReq{UserIds: userIds, OrgId: orgID})
	if err != nil {
		log.Errorf("GetByIds batch failed: %v", err)
		return result
	}
	for _, p := range res.Profiles {
		result[p.UserId] = userInfo{Name: p.Name, Avatar: p.Avatar}
	}
	return result
}

func (c ContestLogService) GetUserContestHistory(ctx context.Context, req *contest_log.GetUserContestHistoryReq) (*contest_log.GetUserContestHistoryRes, error) {
	logs, err := c.sbDal.GetContestByUserId(ctx, req.UserId, req.Cursor, req.Limit, req.Platform)
	if err != nil {
		return nil, errors.InternalServer("内部服务器错误", "服务暂时不可用")
	}

	items := make([]*contest_log.ContestLog, 0, len(logs))
	for _, v := range logs {
		items = append(items, &contest_log.ContestLog{
			Id:          uint32(v.ID),
			Platform:    v.Platform,
			ContestId:   v.ContestId,
			ContestName: v.ContestName,
			ContestUrl:  v.ContestUrl,
			TotalCount:  int32(v.TotalCount),
			Time:        v.Time.Unix(),
		})
	}

	return &contest_log.GetUserContestHistoryRes{
		Code:    0,
		Message: "OK",
		Data:    items,
	}, nil
}

func NewContestLogService(sbDal *dal.SpiderDal, data *data.Data, reg *discovery.Register) *ContestLogService {
	return &ContestLogService{
		sbDal: sbDal,
		db:    data.DB,
		rdb:   data.RDB,
		reg:   &reg.Reg,
	}
}
