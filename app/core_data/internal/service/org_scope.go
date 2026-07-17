package service

import (
	"context"
	"fmt"

	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/core_data/internal/userrpc"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
)

// ResolveOrgMemberIDs 解析组织成员 userId 列表。
// orgID=0 时用 JWT 当前组织；仍为 0 则 user 服务回落公共域。
// scopeSite=true 且站点管理员：unrestricted=true 表示全站。
func ResolveOrgMemberIDs(ctx context.Context, reg *registry.Registrar, orgID uint, scopeSite bool) (userIDs []int64, resolvedOrg uint, unrestricted bool, err error) {
	if scopeSite && auth.VerifySiteAdmin(ctx) {
		return nil, 0, true, nil
	}
	if orgID == 0 {
		if pd := auth.GetCurrentUser(ctx); pd != nil && pd.OrgID > 0 {
			orgID = pd.OrgID
		}
	}
	client, err := userrpc.ProfileClient(reg)
	if err != nil {
		return nil, orgID, false, err
	}
	res, err := client.GetUserIdsByOrg(ctx, &profile.GetUserIdsByOrgReq{OrgId: int64(orgID)})
	if err != nil {
		log.Warnf("GetUserIdsByOrg: %v", err)
		return nil, orgID, false, err
	}
	ids := res.GetUserIds()
	if ids == nil {
		ids = []int64{}
	}
	return ids, uint(res.GetOrgId()), false, nil
}

// fetchFollowingIDs 某人关注的 userId 列表
func fetchFollowingIDs(ctx context.Context, reg *registry.Registrar, userID int64) []int64 {
	if reg == nil || userID <= 0 {
		return []int64{}
	}
	client, err := userrpc.ProfileClient(reg)
	if err != nil {
		log.Warnf("following ids dial: %v", err)
		return []int64{}
	}
	res, err := client.GetFollowingIds(ctx, &profile.GetFollowingIdsReq{UserId: userID})
	if err != nil {
		log.Warnf("GetFollowingIds: %v", err)
		return []int64{}
	}
	ids := res.GetUserIds()
	if ids == nil {
		return []int64{}
	}
	return ids
}

// filterPublicFeedUserIDs 公共域动态：剔除关闭动态可见的用户。
// 调用失败时 fail-closed（返回空），避免隐私过滤失效时全量泄露。
func filterPublicFeedUserIDs(ctx context.Context, reg *registry.Registrar, userIDs []int64) []int64 {
	if reg == nil || len(userIDs) == 0 {
		return []int64{}
	}
	client, err := userrpc.ProfileClient(reg)
	if err != nil {
		log.Warnf("filter public feed dial: %v", err)
		return []int64{}
	}
	res, err := client.FilterPublicFeedUserIds(ctx, &profile.FilterPublicFeedUserIdsReq{UserIds: userIDs})
	if err != nil {
		log.Warnf("FilterPublicFeedUserIds: %v", err)
		return []int64{}
	}
	ids := res.GetUserIds()
	if ids == nil {
		return []int64{}
	}
	return ids
}

// isPublicOrgContext orgID=0 或公共域 slug → 隐私生效
func isPublicOrgContext(ctx context.Context, reg *registry.Registrar, orgID uint) bool {
	if orgID == 0 {
		return true
	}
	if reg == nil {
		return true
	}
	client, err := userrpc.ProfileClient(reg)
	if err != nil {
		return true
	}
	// public org：GetUserIdsByOrg(orgId=0) 回落公共域，对比 id
	pub, err := client.GetUserIdsByOrg(ctx, &profile.GetUserIdsByOrgReq{OrgId: 0})
	if err != nil {
		return true
	}
	return uint(pub.GetOrgId()) == orgID
}

func intersectIDs(a, b []int64) []int64 {
	if len(a) == 0 || len(b) == 0 {
		return []int64{}
	}
	set := make(map[int64]struct{}, len(b))
	for _, id := range b {
		set[id] = struct{}{}
	}
	out := make([]int64, 0, len(a))
	for _, id := range a {
		if _, ok := set[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

// ResolveOrgMemberIDsFromConn 复用已有 user 连接
func ResolveOrgMemberIDsFromConn(ctx context.Context, client profile.ProfileClient, orgID uint, scopeSite bool) ([]int64, uint, bool, error) {
	if scopeSite && auth.VerifySiteAdmin(ctx) {
		return nil, 0, true, nil
	}
	if orgID == 0 {
		if pd := auth.GetCurrentUser(ctx); pd != nil && pd.OrgID > 0 {
			orgID = pd.OrgID
		}
	}
	if client == nil {
		return nil, orgID, false, fmt.Errorf("profile client nil")
	}
	res, err := client.GetUserIdsByOrg(ctx, &profile.GetUserIdsByOrgReq{OrgId: int64(orgID)})
	if err != nil {
		return nil, orgID, false, err
	}
	ids := res.GetUserIds()
	if ids == nil {
		ids = []int64{}
	}
	return ids, uint(res.GetOrgId()), false, nil
}
