package service

import (
	"context"
	"cwxu-algo/api/core/v1/submit_log"
	"cwxu-algo/app/core_data/internal/data/dal"

	"github.com/go-kratos/kratos/v2/errors"
)

type SubmitLogService struct {
	sbDal *dal.SpiderDal
}

func (s SubmitLogService) GetSubmitLog(ctx context.Context, req *submit_log.GetSubmitLogReq) (*submit_log.GetSubmitLogRes, error) {
	data, err := s.sbDal.GetByUserId(ctx, req.UserId, req.Cursor, req.Limit)
	if err != nil {
		return nil, errors.InternalServer("内部服务器错误", err.Error())
	}
	r := make([]*submit_log.SubmitLog, 0)
	for _, v := range data {
		r = append(r, &submit_log.SubmitLog{
			Id:       uint32(v.ID),
			UserId:   v.UserID,
			SubmitId: v.SubmitID,
			Contest:  v.Contest,
			Problem:  v.Problem,
			Lang:     v.Lang,
			Status:   v.Status,
			Time:     v.Time.Unix(),
		})
	}
	return &submit_log.GetSubmitLogRes{
		Data: r,
	}, nil
}

func NewSubmitLogService(sbDal *dal.SpiderDal) *SubmitLogService {
	return &SubmitLogService{
		sbDal: sbDal,
	}
}
