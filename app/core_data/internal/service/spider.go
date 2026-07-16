package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cwxu-algo/api/core/v1/spider"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/common/utils/ratelimit"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"
	spiderregistry "cwxu-algo/app/core_data/internal/spider"
	"cwxu-algo/app/core_data/task"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

var (
	SetForbidden    = errors.Forbidden("权限错误", "权限不允许，设置失败")
	InternalError   = errors.InternalServer("内部错误", "内部错误，操作失败")
	UpdateForbidden = errors.Forbidden("权限错误", "仅站点管理员可手动同步 OJ 数据")
	RateLimitError  = errors.New(429, "TOO_MANY_REQUESTS", "请求过于频繁，请稍后再试")
)

type SpiderService struct {
	spider.UnimplementedSpiderServer
	db     *gorm.DB
	rdb    *redis.Client
	spider *task.SpiderTask
}

func (s SpiderService) allow(ctx context.Context, key string, interval time.Duration) bool {
	ok, err := ratelimit.Allow(ctx, s.rdb, key, interval)
	if err != nil {
		log.Warnf("spider rate limit redis error (allow): %v", err)
	}
	return ok
}

func (s SpiderService) Update(ctx context.Context, req *spider.UpdateReq) (*spider.UpdateRes, error) {
	// 仅站点管理员可手动触发全量同步（普通用户依赖定时任务与绑定后自动抓取）
	if !auth.VerifyAdmin(ctx) {
		return nil, UpdateForbidden
	}
	if !s.allow(ctx, ratelimit.SpiderUpdateKey(req.UserId), 60*time.Second) {
		return nil, RateLimitError
	}
	s.spider.Do(req.UserId, true) // 全量更新该用户全部已绑定平台
	return &spider.UpdateRes{
		Code:    0,
		Message: "更新成功，请稍等片刻，该用户的全量 OJ 数据正在同步",
	}, nil
}

// UpdateAll 管理员一键触发所有已绑定 OJ 用户的全量更新（分批入队，削峰）
func (s SpiderService) UpdateAll(ctx context.Context, _ *spider.UpdateAllReq) (*spider.UpdateAllRes, error) {
	if !auth.VerifyAdmin(ctx) {
		return nil, SetForbidden
	}
	adminId := int64(auth.GetCurrentUserId(ctx))
	if !s.allow(ctx, ratelimit.SpiderUpdateAllKey(adminId), 5*time.Minute) {
		return nil, RateLimitError
	}

	var userIds []int64
	if err := s.db.Model(&model.Platform{}).
		Distinct("user_id").
		Pluck("user_id", &userIds).Error; err != nil {
		log.Errorf("UpdateAll: query platform users failed: %v", err)
		return nil, InternalError
	}

	// 异步分批：API 立即返回，后台按 20 人/分钟 入队
	go s.spider.DoBatch(context.Background(), userIds, true, 20, time.Minute)

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
	if !auth.VerifySelfOrAbove(ctx, uint(req.UserId)) {
		return nil, SetForbidden
	}
	if !s.allow(ctx, ratelimit.SpiderSetKey(req.UserId), 30*time.Second) {
		return nil, RateLimitError
	}
	platformName := strings.TrimSpace(req.Platform)
	username := strings.TrimSpace(req.Username)
	if _, ok := spiderregistry.Get(platformName); !ok {
		return nil, errors.BadRequest("参数错误", "不支持该 OJ 平台")
	}
	if username == "" || len([]rune(username)) > 128 {
		return nil, errors.BadRequest("参数错误", "OJ 用户名不能为空且最多 128 个字符")
	}
	platform := model.Platform{
		UserID:   req.UserId,
		Platform: platformName,
		Username: username,
	}
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("user_id = ? AND platform = ?", req.UserId, platformName).Delete(&model.Platform{}).Error; err != nil {
			return err
		}
		if err := tx.Where("user_id = ? AND platform = ?", req.UserId, platformName).Delete(&model.SubmitLog{}).Error; err != nil {
			return err
		}
		if err := tx.Where("user_id = ? AND platform = ?", req.UserId, platformName).Delete(&model.ContestLog{}).Error; err != nil {
			return err
		}
		return tx.Create(&platform).Error
	}); err != nil {
		log.Errorf("SetSpider transaction failed: %v", err)
		return nil, InternalError
	}
	if err := s.rdb.Del(ctx, fmt.Sprintf("core:submit_log:user:%d", req.UserId)).Err(); err != nil {
		log.Errorf("SetSpider: redis del failed: %v", err)
	}
	_ = s.rdb.Incr(ctx, fmt.Sprintf("core:contest_log:user:%d:ver", req.UserId)).Err()
	// 只全量抓取刚绑定的这一平台，避免重绑 CF 时把其它 OJ 再扫一遍
	s.spider.DoPlatform(req.UserId, platformName, true)
	return &spider.SetSpiderRep{
		Code:    0,
		Message: fmt.Sprintf("绑定成功，正在同步 %s 的全量数据，请稍候", platformName),
	}, nil
}

// PurgeUserData 硬删除用户在 core 库的全部关联数据（删除用户时调用）
func (s SpiderService) PurgeUserData(ctx context.Context, req *spider.PurgeUserDataReq) (*spider.PurgeUserDataRes, error) {
	if req.UserId <= 0 {
		return &spider.PurgeUserDataRes{Code: 1, Message: "用户ID无效"}, nil
	}
	uid := req.UserId
	if err := s.db.WithContext(ctx).Where("user_id = ?", uid).Delete(&model.Platform{}).Error; err != nil {
		log.Errorf("PurgeUserData: platform user=%d: %v", uid, err)
		return nil, InternalError
	}
	if err := s.db.WithContext(ctx).Where("user_id = ?", uid).Delete(&model.SubmitLog{}).Error; err != nil {
		log.Errorf("PurgeUserData: submit_log user=%d: %v", uid, err)
		return nil, InternalError
	}
	if err := s.db.WithContext(ctx).Where("user_id = ?", uid).Delete(&model.ContestLog{}).Error; err != nil {
		log.Errorf("PurgeUserData: contest_log user=%d: %v", uid, err)
		return nil, InternalError
	}
	// 缓存 / 爬虫 inflight
	keys := []string{
		fmt.Sprintf("core:submit_log:user:%d", uid),
		fmt.Sprintf("spider:pending:%d", uid),
		fmt.Sprintf("spider:inflight:%d", uid),
		fmt.Sprintf("user:%d:profile", uid),
	}
	if err := s.rdb.Del(ctx, keys...).Err(); err != nil {
		log.Warnf("PurgeUserData: redis del user=%d: %v", uid, err)
	}
	// 按平台的 pending/inflight 键（扫描可能较多，仅删常见平台前缀）
	for _, p := range []string{"AtCoder", "Codeforces", "LuoGu", "NowCoder", "QOJ", "LeetCode"} {
		_ = s.rdb.Del(ctx,
			fmt.Sprintf("spider:pending:%d:%s", uid, p),
			fmt.Sprintf("spider:inflight:%d:%s", uid, p),
		).Err()
	}
	return &spider.PurgeUserDataRes{Code: 0, Message: "已清空该用户的训练与绑定数据"}, nil
}

func NewSpiderService(data *data.Data, spider *task.SpiderTask) *SpiderService {
	return &SpiderService{
		db:     data.DB,
		rdb:    data.RDB,
		spider: spider,
	}
}
