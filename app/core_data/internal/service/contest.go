package service

import (
	"context"
	"cwxu-algo/api/core/v1/contest_log"
	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/dal"
	"cwxu-algo/app/core_data/internal/data/model"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/redis/go-redis/v9"
	grpc2 "google.golang.org/grpc"
	"gorm.io/gorm"
)

type ContestLogService struct {
	contest_log.UnimplementedContestServer
	sbDal *dal.SpiderDal
	db    *gorm.DB
	rdb   *redis.Client
	reg   *registry.Registrar
}

func (c ContestLogService) userRPC() (*grpc2.ClientConn, error) {
	return grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery((*c.reg).(registry.Discovery)),
		grpc.WithTimeout(20*time.Second),
	)
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
			ContestId:   v.ContestId,
			ContestName: v.ContestName,
			ContestUrl:  v.ContestUrl,
			TotalCount:  int32(v.TotalCount),
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
	contest := model.ContestLog{}
	_ = c.db.Where("id = ?", req.ContestId).First(&contest)
	logs, total, err := c.sbDal.GetContestRanking(ctx, contest.ContestId, contest.Platform, req.Offset, req.Limit)
	if err != nil {
		return nil, errors.InternalServer("内部服务器错误", err.Error())
	}

	items := make([]*contest_log.RankingItem, 0, len(logs))
	nameMap := map[int64]string{}
	for _, v := range logs {
		if _, ok := nameMap[v.UserID]; !ok {
			conn, _ := c.userRPC()
			sb := profile.NewProfileClient(conn)
			res, err := sb.GetById(
				context.Background(),
				&profile.GetByIdReq{UserId: v.UserID},
			)
			if err != nil {
				log.Error(err)
			}
			nameMap[v.UserID] = res.Name
		}
		items = append(items, &contest_log.RankingItem{
			Rank:       int64(v.Rank),
			UserId:     v.UserID,
			Name:       nameMap[v.UserID],
			AcCount:    int32(v.AcCount),
			TotalCount: int32(v.TotalCount),
		})
	}

	return &contest_log.GetContestRankingRes{
		Code:    0,
		Message: "OK",
		Contest: &contest_log.ContestLog{
			Id:          uint32(contest.ID),
			Platform:    contest.Platform,
			ContestId:   contest.ContestId,
			ContestName: contest.ContestName,
			ContestUrl:  contest.ContestUrl,
			TotalCount:  int32(contest.TotalCount),
			Time:        contest.Time.Unix(),
		},
		Data:  items,
		Total: total,
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
			ContestId:   v.ContestId,
			ContestName: v.ContestName,
			ContestUrl:  v.ContestUrl,
			TotalCount:  int32(v.TotalCount),
			Time:        v.Time.Unix(),
		})
	}

	return &contest_log.GetUserContestHistoryRes{
		Code:    0,
		Message: "OK",
		Data:    items,
	}, nil
}

func NewContestLogService(sbDal *dal.SpiderDal, data *data.Data, reg *discovery.Register) *ContestLogService {
	return &ContestLogService{
		sbDal: sbDal,
		db:    data.DB,
		rdb:   data.RDB,
		reg:   &reg.Reg,
	}
}
