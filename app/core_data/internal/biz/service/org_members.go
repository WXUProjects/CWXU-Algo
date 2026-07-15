package service

import (
	"context"
	"fmt"
	"time"

	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/utils/auth"

	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
)

// fetchOrgMemberIDs 通过 user 服务取组织成员
func fetchOrgMemberIDs(ctx context.Context, reg *registry.Registrar, orgID uint) ([]int64, uint, bool, error) {
	if reg == nil {
		return nil, 0, false, fmt.Errorf("registry nil")
	}
	if orgID == 0 {
		if pd := auth.GetCurrentUser(ctx); pd != nil && pd.OrgID > 0 {
			orgID = pd.OrgID
		}
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
		return nil, orgID, false, err
	}
	ids := res.GetUserIds()
	if ids == nil {
		ids = []int64{}
	}
	return ids, uint(res.GetOrgId()), false, nil
}
