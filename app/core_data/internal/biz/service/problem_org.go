package service

import (
	"context"
	"fmt"
	"time"

	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
)

// nonPublicOrgUserCacheTTL 非公共域组织用户集合缓存
const nonPublicOrgUserCacheTTL = 2 * time.Minute

// problemHasOrgSubmitter 该题近 backfillWindow 内是否有「组织用户」（非纯公共域）提交。
// 仅公共域/散户提交历史 → false（只爬题面，不跑题面 AI）。
func (uc *ProblemUseCase) problemHasOrgSubmitter(problemID uint) bool {
	if problemID == 0 {
		return false
	}
	orgUsers, ok := uc.nonPublicOrgUserIDs()
	if !ok {
		// 拉不到组织名单时保守放行，避免 user 短暂故障拖死 AI 流水线
		log.Warnf("problemHasOrgSubmitter: org user list unavailable, allow analyze id=%d", problemID)
		return true
	}
	if len(orgUsers) == 0 {
		return false
	}
	ids := make([]int64, 0, len(orgUsers))
	for id := range orgUsers {
		ids = append(ids, id)
	}
	cutoff := time.Now().Add(-backfillWindow)
	var n int64
	err := uc.data.DB.Model(&model.SubmitLog{}).
		Where("problem_id = ?", problemID).
		Where("time IS NOT NULL AND time >= ?", cutoff).
		Where("user_id IN ?", ids).
		Limit(1).
		Count(&n).Error
	if err != nil {
		log.Warnf("problemHasOrgSubmitter query id=%d: %v", problemID, err)
		return true
	}
	return n > 0
}

func (uc *ProblemUseCase) nonPublicOrgUserIDs() (map[int64]struct{}, bool) {
	uc.orgUsersMu.Lock()
	defer uc.orgUsersMu.Unlock()
	if uc.orgUsersCache != nil && time.Since(uc.orgUsersAt) < nonPublicOrgUserCacheTTL {
		return uc.orgUsersCache, true
	}
	ids, err := uc.fetchNonPublicOrgUserIDs()
	if err != nil {
		log.Warnf("fetchNonPublicOrgUserIDs: %v", err)
		if uc.orgUsersCache != nil {
			return uc.orgUsersCache, true
		}
		return nil, false
	}
	m := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		m[id] = struct{}{}
	}
	uc.orgUsersCache = m
	uc.orgUsersAt = time.Now()
	return m, true
}

func (uc *ProblemUseCase) fetchNonPublicOrgUserIDs() ([]int64, error) {
	if uc.reg == nil {
		return nil, fmt.Errorf("registry nil")
	}
	conn, err := grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery((*uc.reg).(registry.Discovery)),
		grpc.WithTimeout(15*time.Second),
	)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := profile.NewProfileClient(conn)
	res, err := client.GetNonPublicOrgUserIds(context.Background(), &profile.GetNonPublicOrgUserIdsReq{})
	if err != nil {
		return nil, err
	}
	ids := res.GetUserIds()
	if ids == nil {
		ids = []int64{}
	}
	return ids, nil
}
