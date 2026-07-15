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
