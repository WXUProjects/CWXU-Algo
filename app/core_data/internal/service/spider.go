package service

import (
	"context"
	"cwxu-algo/api/core/v1/spider"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/task"
	"fmt"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
	"gorm.io/gorm"
)

var (
	SetForbidden    = errors.Forbidden("权限错误", "权限不允许，设置失败")
	InternalError   = errors.InternalServer("内部错误", "内部错误，操作失败")
	UpdateForbidden = errors.Forbidden("权限错误", "权限不允许，不允许手动申请全量更新他人数据")
	RateLimitError  = errors.New(429, "TOO_MANY_REQUESTS", "请求过于频繁，请稍后再试")
)

type SpiderService struct {
	spider.UnimplementedSpiderServer
	db          *gorm.DB
	rdb         *redis.Client
	spider      *task.SpiderTask
	limiterMap  sync.Map // map[int64]*rate.Limiter
}

func (s *SpiderService) getLimiter(userId int64, interval time.Duration) *rate.Limiter {
	if l, ok := s.limiterMap.Load(userId); ok {
		return l.(*rate.Limiter)
	}
	// 1 request per interval, burst 1
	l := rate.NewLimiter(rate.Every(interval), 1)
	actual, _ := s.limiterMap.LoadOrStore(userId, l)
	return actual.(*rate.Limiter)
}

func (s SpiderService) Update(ctx context.Context, req *spider.UpdateReq) (*spider.UpdateRes, error) {
	//if !auth.VerifyById(ctx, uint(req.UserId)) {
	//	return nil, UpdateForbidden
	//}
	limiter := s.getLimiter(req.UserId, 60*time.Second)
	if !limiter.Allow() {
		return nil, RateLimitError
	}
	s.spider.Do(req.UserId, true) // 全量更新
	return &spider.UpdateRes{
		Code:    0,
		Message: "更新成功，请稍等片刻，您的全量OJ数据正在更新",
	}, nil
}

// UpdateAll 管理员一键触发所有已绑定 OJ 用户的全量更新
func (s SpiderService) UpdateAll(ctx context.Context, _ *spider.UpdateAllReq) (*spider.UpdateAllRes, error) {
	if !auth.VerifyAdmin(ctx) {
		return nil, SetForbidden
	}
	// 全站按钮限流：同一管理员 5 分钟内只能点一次
	adminId := int64(auth.GetCurrentUserId(ctx))
	limiter := s.getLimiter(-adminId, 5*time.Minute) // 负 id 与用户更新限流隔离
	if !limiter.Allow() {
		return nil, RateLimitError
	}

	var userIds []int64
	if err := s.db.Model(&model.Platform{}).
		Distinct("user_id").
		Pluck("user_id", &userIds).Error; err != nil {
		log.Errorf("UpdateAll: query platform users failed: %v", err)
		return nil, InternalError
	}

	for _, uid := range userIds {
		s.spider.Do(uid, true)
	}

	return &spider.UpdateAllRes{
		Code:    0,
		Message: fmt.Sprintf("已为 %d 名用户触发全量更新，后台异步抓取中", len(userIds)),
		Count:   int64(len(userIds)),
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
	// 校验JWT：只能设置自己的 spider，或者管理员可以设置任何人
	if !auth.VerifySelfOrAbove(ctx, uint(req.UserId)) {
		return nil, SetForbidden
	}
	// Rate limit
	limiter := s.getLimiter(req.UserId, 30*time.Second)
	if !limiter.Allow() {
		return nil, RateLimitError
	}
	// 直接设置进去 构建Platform
	platform := model.Platform{
		UserID:   req.UserId,
		Platform: req.Platform,
		Username: req.Username,
	}
	if err := s.db.Where("user_id = ? AND platform = ?", req.UserId, req.Platform).Delete(&model.Platform{}).Error; err != nil {
		log.Errorf("SetSpider: delete platform failed: %v", err)
	}
	if err := s.db.Where("user_id = ? AND platform = ?", req.UserId, req.Platform).Delete(&model.SubmitLog{}).Error; err != nil {
		log.Errorf("SetSpider: delete submit_log failed: %v", err)
	}
	if err := s.rdb.Del(ctx, fmt.Sprintf("core:submit_log:user:%d", req.UserId)).Err(); err != nil {
		log.Errorf("SetSpider: redis del failed: %v", err)
	}
	err := s.db.Save(&platform).Error
	if err != nil {
		log.Errorf("SetSpider: save platform failed: %v", err)
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
