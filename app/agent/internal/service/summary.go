package service

import (
	"context"
	"cwxu-algo/api/agent/v1/summary"
	"cwxu-algo/app/agent/internal/data"
	"cwxu-algo/app/common/event"
	"cwxu-algo/app/core_data/task"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type SummaryService struct {
	rdb      *redis.Client
	rabbitMQ *event.RabbitMQ
}

func (s SummaryService) GetRecentSummary(ctx context.Context, request *summary.GetSummaryRequest) (*summary.GetSummaryReply, error) {
	key := fmt.Sprintf("agent:summary:%d:recent", request.UserId)
	val, err := s.rdb.Get(ctx, key).Result()
	if err != nil {
		st := task.NewSummaryTask(s.rabbitMQ)
		st.Do(request.UserId, "PersonalRecent")
		return &summary.GetSummaryReply{
			Code: 1,
			Msg:  "嘿嘿，稍等稍等，您的 AI 分析报告马上就好(1-2min)",
			Resp: "",
		}, nil
	}
	return &summary.GetSummaryReply{
		Code: 0,
		Msg:  "success",
		Resp: val,
	}, nil
}

func NewSummaryService(data *data.Data, rabbitMQ *event.RabbitMQ) *SummaryService {
	return &SummaryService{
		rdb:      data.RDB,
		rabbitMQ: rabbitMQ,
	}
}
