package service

import (
	"context"

	"cwxu-algo/api/core/v1/statistic"
	"cwxu-algo/app/core_data/internal/biz/service"

	"github.com/go-kratos/kratos/v2/errors"
)

// StatisticService 统计服务
type StatisticService struct {
	statistic.UnimplementedStatisticServer
	uc *service.StatisticUseCase
}

// NewStatistic 创建统计服务
func NewStatistic(uc *service.StatisticUseCase) *StatisticService {
	return &StatisticService{
		UnimplementedStatisticServer: statistic.UnimplementedStatisticServer{},
		uc:                          uc,
	}
}

// Rank 获取排行榜数据
// 参数来自 statistic.proto:
// - userId: 用户ID，0表示所有用户
// - time: 时间维度 (日/周/月)
// - scoreType: 分数类型 ("ac"表示AC排行榜，其他表示提交排行榜)
// - groupId: 分组ID，-1表示所有分组
// - page: 页码
// - pageSize: 每页大小
func (s *StatisticService) Rank(ctx context.Context, req *statistic.RankReq) (*statistic.RankResp, error) {
	if req.Time == "" {
		return nil, errors.BadRequest("参数错误", "time参数不能为空")
	}
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	}
	if req.PageSize > 100 {
		req.PageSize = 100
	}

	return s.uc.Rank(ctx, req)
}

// Heatmap 获取热力图数据
func (s *StatisticService) Heatmap(ctx context.Context, req *statistic.HeatmapReq) (*statistic.HeatmapResp, error) {
	return s.uc.Heatmap(ctx, req)
}

// PeriodCount 获取时间段统计数据
func (s *StatisticService) PeriodCount(ctx context.Context, req *statistic.PeriodCountReq) (*statistic.PeriodCountResp, error) {
	return s.uc.PeriodCount(ctx, req)
}
