package service

import (
	"context"
	"cwxu-algo/api/core/v1/contest_log"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/dal"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

type ContestLogService struct {
	contest_log.UnimplementedContestServer
	sbDal *dal.SpiderDal
	db    *gorm.DB
	rdb   *redis.Client
}

func (c ContestLogService) GetContestList(ctx context.Context, req *contest_log.GetContestListReq) (*contest_log.GetContestListRes, error) {
	logs, total, err := c.sbDal.GetContestList(ctx, req.UserId, req.Offset, req.Limit, req.Platform)
	if err != nil {
		return nil, errors.InternalServer("内部服务器错误", err.Error())
	}

	items := make([]*contest_log.ContestLog, 0, len(logs))
	for _, v := range logs {
		items = append(items, &contest_log.ContestLog{
			Id:          uint32(v.ID),
			Platform:    v.Platform,
			UserId:      v.UserID,
			ContestId:   v.ContestId,
			ContestName: v.ContestName,
			ContestUrl:  v.ContestUrl,
			Rank:        int32(v.Rank),
			TotalCount:  int32(v.TotalCount),
			AcCount:     int32(v.AcCount),
			Time:        v.Time.Unix(),
		})
	}

	return &contest_log.GetContestListRes{
		Code:    0,
		Message: "OK",
		Data:    items,
		Total:   total,
	}, nil
}

func (c ContestLogService) GetContestRanking(ctx context.Context, req *contest_log.GetContestRankingReq) (*contest_log.GetContestRankingRes, error) {
	logs, total, err := c.sbDal.GetContestRanking(ctx, req.ContestId, req.Offset, req.Limit)
	if err != nil {
		return nil, errors.InternalServer("内部服务器错误", err.Error())
	}

	items := make([]*contest_log.RankingItem, 0, len(logs))
	for _, v := range logs {
		items = append(items, &contest_log.RankingItem{
			Rank:       int64(v.Rank),
			UserId:     v.UserID,
			AcCount:    int32(v.AcCount),
			TotalCount: int32(v.TotalCount),
		})
	}

	return &contest_log.GetContestRankingRes{
		Code:    0,
		Message: "OK",
		Data:    items,
		Total:   total,
	}, nil
}

func (c ContestLogService) GetUserContestHistory(ctx context.Context, req *contest_log.GetUserContestHistoryReq) (*contest_log.GetUserContestHistoryRes, error) {
	logs, err := c.sbDal.GetContestByUserId(ctx, req.UserId, req.Cursor, req.Limit, req.Platform)
	if err != nil {
		return nil, errors.InternalServer("内部服务器错误", err.Error())
	}

	items := make([]*contest_log.ContestLog, 0, len(logs))
	for _, v := range logs {
		items = append(items, &contest_log.ContestLog{
			Id:          uint32(v.ID),
			Platform:    v.Platform,
			UserId:      v.UserID,
			ContestId:   v.ContestId,
			ContestName: v.ContestName,
			ContestUrl:  v.ContestUrl,
			Rank:        int32(v.Rank),
			TotalCount:  int32(v.TotalCount),
			AcCount:     int32(v.AcCount),
			Time:        v.Time.Unix(),
		})
	}

	return &contest_log.GetUserContestHistoryRes{
		Code:    0,
		Message: "OK",
		Data:    items,
	}, nil
}

func NewContestLogService(sbDal *dal.SpiderDal, data *data.Data) *ContestLogService {
	return &ContestLogService{
		sbDal: sbDal,
		db:    data.DB,
		rdb:   data.RDB,
	}
}
