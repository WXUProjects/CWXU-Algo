package service

import (
	"context"
	"cwxu-algo/api/core/v1/submit_log"
	"cwxu-algo/app/common/utils"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/dal"
	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/errors"
	"gorm.io/gorm"
)

type SubmitLogService struct {
	submit_log.UnimplementedSubmitServer
	sbDal *dal.SpiderDal
	db    *gorm.DB
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
			Platform: v.Platform,
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

func (s SubmitLogService) LastSubmitTime(ctx context.Context, req *submit_log.LastSubmitTimeReq) (*submit_log.LastSubmitTimeRes, error) {
	var d []model.SubmitLog
	err := s.db.
		Table("submit_logs").
		Select("DISTINCT ON (user_id) user_id, time").
		Where("user_id IN ?", req.UserIds).
		Order("user_id, time DESC").
		Scan(&d).Error
	timesMap := make(map[int64]int64)
	for _, v := range d {
		timesMap[v.UserID] = v.Time.Unix()
	}
	encoded, err := utils.GobEncoder(timesMap)
	if err != nil {
		return nil, errors.InternalServer("内部错误", "编码错误")
	}
	return &submit_log.LastSubmitTimeRes{TimeMap: encoded}, nil
}

func NewSubmitLogService(sbDal *dal.SpiderDal, data *data.Data) *SubmitLogService {
	return &SubmitLogService{
		sbDal: sbDal,
		db:    data.DB,
	}
}
