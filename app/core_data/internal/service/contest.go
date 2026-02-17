package service

import (
	"context"
	"cwxu-algo/api/core/v1/contest_log"
	"cwxu-algo/api/core/v1/submit_log"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/dal"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

type ContestLogService struct {
	submit_log.UnimplementedSubmitServer
	sbDal *dal.SpiderDal
	db    *gorm.DB
	rdb   *redis.Client
}

func (c ContestLogService) GetContestList(ctx context.Context, req *contest_log.GetContestListReq) (*contest_log.GetContestListRes, error) {
	//TODO implement me
	panic("implement me")
}

func (c ContestLogService) GetContestRanking(ctx context.Context, req *contest_log.GetContestRankingReq) (*contest_log.GetContestRankingRes, error) {
	//TODO implement me
	panic("implement me")
}

func (c ContestLogService) GetUserContestHistory(ctx context.Context, req *contest_log.GetUserContestHistoryReq) (*contest_log.GetUserContestHistoryRes, error) {
	//TODO implement me
	panic("implement me")
}

func NewContestLogService(sbDal *dal.SpiderDal, data *data.Data) *ContestLogService {
	return &ContestLogService{
		sbDal: sbDal,
		db:    data.DB,
		rdb:   data.RDB,
	}
}
