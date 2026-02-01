package service

import (
	"context"

	"cwxu-algo/api/core/v1/statistic"
	"cwxu-algo/app/core_data/internal/biz/service"
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
		uc:                           uc,
	}
}

// Heatmap 获取热力图数据
func (s *StatisticService) Heatmap(ctx context.Context, req *statistic.HeatmapReq) (*statistic.HeatmapResp, error) {
	return s.uc.Heatmap(ctx, req)
}

// PeriodCount 获取时间段统计数据
func (s *StatisticService) PeriodCount(ctx context.Context, req *statistic.PeriodCountReq) (*statistic.PeriodCountResp, error) {
	return s.uc.PeriodCount(ctx, req)
}
