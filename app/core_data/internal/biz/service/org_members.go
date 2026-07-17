package service

import (
	"context"
	"fmt"

	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/core_data/internal/userrpc"

	"github.com/go-kratos/kratos/v2/registry"
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
	client, err := userrpc.ProfileClient(reg)
	if err != nil {
		return nil, orgID, false, err
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

// fetchDisplayNames 批量取当前组织（或指定 org）内展示名
func fetchDisplayNames(ctx context.Context, reg *registry.Registrar, userIDs []int64) map[int64]string {
	out := map[int64]string{}
	if reg == nil || len(userIDs) == 0 {
		return out
	}
	var orgID int64
	if pd := auth.GetCurrentUser(ctx); pd != nil {
		orgID = int64(pd.OrgID)
	}
	client, err := userrpc.ProfileClient(reg)
	if err != nil {
		return out
	}
	res, err := client.GetByIds(ctx, &profile.GetByIdsReq{UserIds: userIDs, OrgId: orgID})
	if err != nil || res == nil {
		return out
	}
	for _, p := range res.Profiles {
		if p.Name != "" {
			out[p.UserId] = p.Name
		}
	}
	return out
}
