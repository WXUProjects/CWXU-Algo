package service

import (
	"context"
	"fmt"
	"time"

	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/utils/auth"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
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
	if reg == nil {
		return nil, orgID, false, fmt.Errorf("registry not configured")
	}
	conn, err := grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery((*reg).(registry.Discovery)),
		grpc.WithTimeout(15*time.Second),
	)
	if err != nil {
		return nil, orgID, false, err
	}
	defer conn.Close()
	client := profile.NewProfileClient(conn)
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
	conn, err := grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery((*reg).(registry.Discovery)),
		grpc.WithTimeout(15*time.Second),
	)
	if err != nil {
		log.Warnf("following ids dial: %v", err)
		return []int64{}
	}
	defer conn.Close()
	client := profile.NewProfileClient(conn)
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
	conn, err := grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery((*reg).(registry.Discovery)),
		grpc.WithTimeout(15*time.Second),
	)
	if err != nil {
		log.Warnf("filter public feed dial: %v", err)
		return []int64{}
	}
	defer conn.Close()
	client := profile.NewProfileClient(conn)
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
	// 无法查 org 表时：若 JWT 当前组织与 resolved 相同且无额外信息，保守按「非公共」不筛隐私
	// 通过 GetUserIdsByOrg 的约定：公共域 id 由 user 服务解析；此处用 org 成员 RPC 无法拿 slug
	// 简化：JWT 无 org 或 org 为公共域时由 user 侧 Filter 无害（私人域调用 Filter 会误伤）
	// 私人域：隐私失效，不应 Filter。用 org 角色旁路：当 resolvedOrg 来自 JWT 且非 0，再查一次是否公共
	// 实现：FilterPublicFeed 在私人域也不该调用；调用方用 isPublicOrgContext
	// 通过 profile GetUserIdsByOrg 无法知 is_system；改为：orgID 与 public 比对
	if reg == nil {
		return true
	}
	conn, err := grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery((*reg).(registry.Discovery)),
		grpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		return true
	}
	defer conn.Close()
	client := profile.NewProfileClient(conn)
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
