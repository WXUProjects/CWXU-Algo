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

// pipelineUserCacheTTL 题面流水线资格用户缓存
const pipelineUserCacheTTL = 2 * time.Minute

// problemHasFetchSubmitter 近窗是否有「题面爬取资格」用户提交
func (uc *ProblemUseCase) problemHasFetchSubmitter(problemID uint) bool {
	return uc.problemHasPipelineSubmitter(problemID, "fetch")
}

// problemHasAISubmitter 近窗是否有「题面 AI 资格」用户提交
func (uc *ProblemUseCase) problemHasAISubmitter(problemID uint) bool {
	return uc.problemHasPipelineSubmitter(problemID, "ai")
}

// problemHasOrgSubmitter 兼容旧调用：等价于 AI 资格（题面 AI 闸门）
func (uc *ProblemUseCase) problemHasOrgSubmitter(problemID uint) bool {
	return uc.problemHasAISubmitter(problemID)
}

func (uc *ProblemUseCase) problemHasPipelineSubmitter(problemID uint, kind string) bool {
	if problemID == 0 {
		return false
	}
	users, ok := uc.pipelineUserIDs(kind)
	if !ok {
		// 拉不到名单时保守放行，避免 user 短暂故障拖死流水线
		log.Warnf("problemHasPipelineSubmitter(%s): list unavailable, allow id=%d", kind, problemID)
		return true
	}
	if len(users) == 0 {
		return false
	}
	ids := make([]int64, 0, len(users))
	for id := range users {
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
		log.Warnf("problemHasPipelineSubmitter(%s) query id=%d: %v", kind, problemID, err)
		return true
	}
	return n > 0
}

// shouldEnqueueFetch 是否入队爬题面：近窗有爬取资格用户提交
func (uc *ProblemUseCase) shouldEnqueueFetch(problemID uint) bool {
	return uc.problemHasFetchSubmitter(problemID)
}

func (uc *ProblemUseCase) pipelineUserIDs(kind string) (map[int64]struct{}, bool) {
	uc.orgUsersMu.Lock()
	defer uc.orgUsersMu.Unlock()
	if uc.pipelineUsersCache != nil && time.Since(uc.pipelineUsersAt) < pipelineUserCacheTTL {
		if kind == "ai" {
			return uc.pipelineUsersCache.ai, true
		}
		return uc.pipelineUsersCache.fetch, true
	}
	fetchIDs, aiIDs, err := uc.fetchPipelineUserIDs()
	if err != nil {
		log.Warnf("fetchPipelineUserIDs: %v", err)
		if uc.pipelineUsersCache != nil {
			if kind == "ai" {
				return uc.pipelineUsersCache.ai, true
			}
			return uc.pipelineUsersCache.fetch, true
		}
		// 回退旧缓存字段
		if uc.orgUsersCache != nil {
			return uc.orgUsersCache, true
		}
		return nil, false
	}
	fetchM := make(map[int64]struct{}, len(fetchIDs))
	for _, id := range fetchIDs {
		fetchM[id] = struct{}{}
	}
	aiM := make(map[int64]struct{}, len(aiIDs))
	for _, id := range aiIDs {
		aiM[id] = struct{}{}
	}
	uc.pipelineUsersCache = &pipelineUserSets{fetch: fetchM, ai: aiM}
	uc.pipelineUsersAt = time.Now()
	// 兼容旧字段
	uc.orgUsersCache = fetchM
	uc.orgUsersAt = uc.pipelineUsersAt
	if kind == "ai" {
		return aiM, true
	}
	return fetchM, true
}

// nonPublicOrgUserIDs 兼容：返回爬取资格集合
func (uc *ProblemUseCase) nonPublicOrgUserIDs() (map[int64]struct{}, bool) {
	return uc.pipelineUserIDs("fetch")
}

type pipelineUserSets struct {
	fetch map[int64]struct{}
	ai    map[int64]struct{}
}

func (uc *ProblemUseCase) fetchPipelineUserIDs() (fetchIDs, aiIDs []int64, err error) {
	if uc.reg == nil {
		return nil, nil, fmt.Errorf("registry nil")
	}
	conn, err := grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery((*uc.reg).(registry.Discovery)),
		grpc.WithTimeout(15*time.Second),
	)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	client := profile.NewProfileClient(conn)
	res, err := client.GetNonPublicOrgUserIds(context.Background(), &profile.GetNonPublicOrgUserIdsReq{})
	if err != nil {
		return nil, nil, err
	}
	fetchIDs = res.GetFetchUserIds()
	if len(fetchIDs) == 0 {
		// 旧 user 服务未返回 fetchUserIds 时回落 userIds
		fetchIDs = res.GetUserIds()
	}
	aiIDs = res.GetAiUserIds()
	if len(aiIDs) == 0 {
		// 旧服务无 ai 列表：与爬取共用
		aiIDs = fetchIDs
	}
	if fetchIDs == nil {
		fetchIDs = []int64{}
	}
	if aiIDs == nil {
		aiIDs = []int64{}
	}
	return fetchIDs, aiIDs, nil
}

func (uc *ProblemUseCase) fetchNonPublicOrgUserIDs() ([]int64, error) {
	fetch, _, err := uc.fetchPipelineUserIDs()
	return fetch, err
}
