package service

import (
	"context"
	"cwxu-algo/api/core/v1/spider"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/task"
	"fmt"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

var (
	SetForbidden    = errors.Forbidden("权限错误", "权限不允许，设置失败")
	InternalError   = errors.InternalServer("内部错误", "内部错误，操作失败")
	UpdateForbidden = errors.Forbidden("权限错误", "权限不允许，不允许手动申请全量更新他人数据")
)

type SpiderService struct {
	spider.UnimplementedSpiderServer
	db     *gorm.DB
	rdb    *redis.Client
	spider *task.SpiderTask
}

func (s SpiderService) Update(ctx context.Context, req *spider.UpdateReq) (*spider.UpdateRes, error) {
	if !auth.VerifyById(ctx, uint(req.UserId)) {
		return nil, UpdateForbidden
	}
	s.spider.Do(req.UserId, true) // 全量更新
	return &spider.UpdateRes{
		Code:    0,
		Message: "更新成功，请稍等片刻，您的全量OJ数据正在更新",
	}, nil
}

func (s SpiderService) GetSpider(ctx context.Context, req *spider.GetSpiderReq) (*spider.GetSpiderRep, error) {
	var plats []model.Platform
	err := s.db.Where("user_id = ?", req.UserId).Find(&plats).Error
	if err != nil {
		return nil, InternalError
	}
	res := make([]*spider.GetSpiderRep_Data, 0)
	for _, v := range plats {
		res = append(res, &spider.GetSpiderRep_Data{
			Platform: v.Platform,
			Username: v.Username,
		})
	}
	return &spider.GetSpiderRep{
		Data: res,
	}, nil
}

func (s SpiderService) SetSpider(ctx context.Context, req *spider.SetSpiderReq) (*spider.SetSpiderRep, error) {
	// 校验JWT
	if !auth.VerifyById(ctx, uint(req.UserId)) {
		return nil, SetForbidden
	}
	// 直接设置进去 构建Platform
	platform := model.Platform{
		UserID:   req.UserId,
		Platform: req.Platform,
		Username: req.Username,
	}
	s.db.Where("user_id = ? AND platform = ?", req.UserId, req.Platform).Delete(&model.Platform{})
	s.db.Where("user_id = ? AND platform = ?", req.UserId, req.Platform).Delete(&model.SubmitLog{})
	s.rdb.Del(ctx, fmt.Sprintf("core:submit_log:user:%d", req.UserId))
	err := s.db.Save(&platform).Error
	if err != nil {
		log.Error("设置失败", err.Error)
		return nil, InternalError
	}
	s.spider.Do(req.UserId, true) // 全量更新
	return &spider.SetSpiderRep{
		Code:    0,
		Message: "设置成功，请稍等片刻，您的全量OJ数据正在更新",
	}, nil
}

func NewSpiderService(data *data.Data, spider *task.SpiderTask) *SpiderService {
	return &SpiderService{
		db:     data.DB,
		rdb:    data.RDB,
		spider: spider,
	}
}
