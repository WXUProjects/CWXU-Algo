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
	"cwxu-algo/app/core_data/internal/data/dal"
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

	// 一次全部入队 MQ；并发消费由 spider consumer Qos=4 控制
	go s.spider.DoBatch(context.Background(), userIds, true, 0, 0)

	return &spider.UpdateAllRes{
		Code:    0,
		Message: fmt.Sprintf("已为 %d 名用户全部入队全量更新，后台并发抓取中", len(userIds)),
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
		// 按平台剪枝预聚合 + 账本（热表仅 6 个月，不可再从残缺明细 Rebuild）
		if err := dal.DeletePlatformDailyStats(ctx, tx, req.UserId, platformName); err != nil {
			return err
		}
		if err := dal.DeletePlatformUserAC(ctx, tx, req.UserId, platformName); err != nil {
			return err
		}
		if err := dal.DeletePlatformCountedIDs(ctx, tx, req.UserId, platformName); err != nil {
			return err
		}
		return tx.Create(&platform).Error
	}); err != nil {
		log.Errorf("SetSpider transaction failed: %v", err)
		return nil, InternalError
	}
	// 缓存与统计版本：立即让首页题量/热力读到重建后的数
	if err := s.rdb.Del(ctx,
		fmt.Sprintf("core:submit_log:user:%d", req.UserId),
		fmt.Sprintf("user:%d:lastSubmitTime", req.UserId),
	).Err(); err != nil {
		log.Errorf("SetSpider: redis del failed: %v", err)
	}
	_ = s.rdb.Incr(ctx, fmt.Sprintf("core:contest_log:user:%d:ver", req.UserId)).Err()
	_ = s.rdb.Incr(ctx, fmt.Sprintf("statistic:user:%d:ver", req.UserId)).Err()
	// 递增代数：正在跑的旧全量任务写入前会发现代数过期并丢弃，避免把已删数据写回叠统计
	s.spider.BumpGeneration(req.UserId, platformName)
	// 强制允许本次入队（旧全量任务可能仍占 pending/inflight）
	s.spider.ResetDedup(req.UserId, platformName)
	// 只全量抓取刚绑定的这一平台，避免重绑 CF 时把其它 OJ 再扫一遍
	s.spider.DoPlatform(req.UserId, platformName, true)
	return &spider.SetSpiderRep{
		Code:    0,
		Message: fmt.Sprintf("绑定成功，正在同步 %s 的全量数据，请稍候", platformName),
	}, nil
}

const purgeSubmitsConfirm = "PURGE_SUBMITS"

// SubmitInventory 运维：真实入库提交库存（仅站点管理员）
func (s SpiderService) SubmitInventory(ctx context.Context, _ *spider.SubmitInventoryReq) (*spider.SubmitInventoryRes, error) {
	if !auth.VerifySiteAdmin(ctx) {
		return nil, errors.Forbidden("权限不足", "仅站点管理员可查看提交库存")
	}
	var total, realTotal, ledger int64
	if err := s.db.WithContext(ctx).Model(&model.SubmitLog{}).Count(&total).Error; err != nil {
		return nil, InternalError
	}
	if err := s.db.WithContext(ctx).Model(&model.SubmitLog{}).
		Where(model.SQLExcludeLeetCodeNonSubmit).
		Count(&realTotal).Error; err != nil {
		return nil, InternalError
	}
	if s.db.Migrator().HasTable(&model.CountedSubmitID{}) {
		_ = s.db.WithContext(ctx).Model(&model.CountedSubmitID{}).Count(&ledger).Error
	}
	var bounds struct {
		Oldest *time.Time
		Newest *time.Time
	}
	_ = s.db.WithContext(ctx).Model(&model.SubmitLog{}).
		Select("MIN(time) AS oldest, MAX(time) AS newest").
		Scan(&bounds).Error
	var oldest, newest int64
	if bounds.Oldest != nil {
		oldest = bounds.Oldest.Unix()
	}
	if bounds.Newest != nil {
		newest = bounds.Newest.Unix()
	}
	return &spider.SubmitInventoryRes{
		Code:                  0,
		Message:               "ok",
		SubmitLogsTotal:       total,
		SubmitLogsRealTotal:   realTotal,
		CountedSubmitIdsTotal: ledger,
		OldestTime:            oldest,
		NewestTime:            newest,
	}, nil
}

// PurgeSubmitsAndRecrawl 运维：清空全部提交相关数据并全量重爬（仅站管）
// 删除 submit_logs / 账本 / 日汇总 / AC 预聚合；保留 platforms 与 contest_logs。
func (s SpiderService) PurgeSubmitsAndRecrawl(ctx context.Context, req *spider.PurgeSubmitsAndRecrawlReq) (*spider.PurgeSubmitsAndRecrawlRes, error) {
	if !auth.VerifySiteAdmin(ctx) {
		return nil, errors.Forbidden("权限不足", "仅站点管理员可执行此运维操作")
	}
	if strings.TrimSpace(req.GetConfirm()) != purgeSubmitsConfirm {
		return &spider.PurgeSubmitsAndRecrawlRes{
			Code:    2,
			Message: "请输入确认口令 PURGE_SUBMITS",
		}, nil
	}
	adminID := int64(auth.GetCurrentUserId(ctx))
	const purgeLockKey = "ops:purge_submits"
	// 全局锁防双点；结束后必须释放（成功/失败），重启服务也会在启动时清掉
	if s.rdb != nil {
		ok, err := s.rdb.SetNX(ctx, purgeLockKey, "1", 30*time.Minute).Result()
		if err != nil {
			log.Warnf("purge_submits lock redis: %v", err)
		} else if !ok {
			return &spider.PurgeSubmitsAndRecrawlRes{
				Code:    3,
				Message: "已有清空任务在进行，请稍后再试",
			}, nil
		} else {
			defer func() { _ = s.rdb.Del(context.Background(), purgeLockKey).Err() }()
		}
	}

	deletedLogs, err := deleteAllInBatches(ctx, s.db, "submit_logs", 5000)
	if err != nil {
		log.Errorf("purge submit_logs: %v", err)
		return nil, InternalError
	}
	var deletedLedger, deletedDaily, deletedAc int64
	if s.db.Migrator().HasTable(&model.CountedSubmitID{}) {
		res := s.db.WithContext(ctx).Where("1 = 1").Delete(&model.CountedSubmitID{})
		if res.Error != nil {
			return nil, InternalError
		}
		deletedLedger = res.RowsAffected
	}
	if s.db.Migrator().HasTable(&model.DailyUserStat{}) {
		res := s.db.WithContext(ctx).Where("1 = 1").Delete(&model.DailyUserStat{})
		if res.Error != nil {
			return nil, InternalError
		}
		deletedDaily = res.RowsAffected
	}
	if s.db.Migrator().HasTable(&model.UserACProblem{}) {
		res := s.db.WithContext(ctx).Where("1 = 1").Delete(&model.UserACProblem{})
		if res.Error != nil {
			return nil, InternalError
		}
		deletedAc += res.RowsAffected
	}
	if s.db.Migrator().HasTable(&model.UserACProblemDay{}) {
		res := s.db.WithContext(ctx).Where("1 = 1").Delete(&model.UserACProblemDay{})
		if res.Error != nil {
			return nil, InternalError
		}
		deletedAc += res.RowsAffected
	}

	// 全局统计版本失效 + 个人 ver 批量 bump 成本高，只 bump 全局
	if s.rdb != nil {
		_ = s.rdb.Incr(ctx, "statistic:heatmap:global:ver").Err()
		_ = s.rdb.Incr(ctx, "statistic:period:global:ver").Err()
	}

	var userIds []int64
	if err := s.db.Model(&model.Platform{}).
		Distinct("user_id").
		Pluck("user_id", &userIds).Error; err != nil {
		log.Errorf("purge recrawl list users: %v", err)
		return nil, InternalError
	}
	// 全部入队全量重爬；并发由 spider consumer 控制
	go s.spider.DoBatch(context.Background(), userIds, true, 0, 0)

	log.Warnf("ops purge-submits admin=%d deleted_logs=%d ledger=%d daily=%d ac=%d enqueued=%d",
		adminID, deletedLogs, deletedLedger, deletedDaily, deletedAc, len(userIds))

	return &spider.PurgeSubmitsAndRecrawlRes{
		Code:              0,
		Message:           fmt.Sprintf("已清空提交相关数据，并为 %d 名已绑定用户触发全量重爬", len(userIds)),
		DeletedSubmitLogs: deletedLogs,
		DeletedLedger:     deletedLedger,
		DeletedDaily:      deletedDaily,
		DeletedAc:         deletedAc,
		EnqueuedUsers:     int64(len(userIds)),
	}, nil
}

// ClearPurgeLock 启动时清除运维 purge 锁（进程挂掉时可能残留）
func ClearPurgeLock(rdb *redis.Client) {
	if rdb == nil {
		return
	}
	if err := rdb.Del(context.Background(), "ops:purge_submits").Err(); err != nil {
		log.Warnf("clear ops:purge_submits: %v", err)
	}
}

// deleteAllInBatches 分批清空表（避免大表长锁）
func deleteAllInBatches(ctx context.Context, db *gorm.DB, table string, batch int) (int64, error) {
	if db == nil || table == "" {
		return 0, nil
	}
	if batch <= 0 {
		batch = 5000
	}
	var total int64
	for {
		res := db.WithContext(ctx).Exec(fmt.Sprintf(`
			DELETE FROM %s
			WHERE ctid IN (
				SELECT ctid FROM %s LIMIT %d
			)
		`, table, table, batch))
		if res.Error != nil {
			return total, res.Error
		}
		total += res.RowsAffected
		if res.RowsAffected == 0 {
			break
		}
	}
	return total, nil
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
	if err := dal.DeleteUserPreagg(ctx, s.db, uid); err != nil {
		log.Errorf("PurgeUserData: preagg user=%d: %v", uid, err)
		return nil, InternalError
	}
	// 缓存 / 爬虫 inflight
	keys := []string{
		fmt.Sprintf("core:submit_log:user:%d", uid),
		fmt.Sprintf("spider:pending:%d", uid),
		fmt.Sprintf("spider:inflight:%d", uid),
		fmt.Sprintf("user:%d:profile", uid),
		fmt.Sprintf("statistic:user:%d:ver", uid),
	}
	if err := s.rdb.Del(ctx, keys...).Err(); err != nil {
		log.Warnf("PurgeUserData: redis del user=%d: %v", uid, err)
	}
	// 按平台的 pending/inflight 键（扫描可能较多，仅删常见平台前缀）
	for _, p := range []string{"AtCoder", "Codeforces", "LuoGu", "NowCoder", "QOJ", "LeetCode"} {
		_ = s.rdb.Del(ctx,
			fmt.Sprintf("spider:pending:%d:%s", uid, p),
			fmt.Sprintf("spider:inflight:%d:%s", uid, p),
			fmt.Sprintf("spider:writelock:%d:%s", uid, p),
		).Err()
	}
	return &spider.PurgeUserDataRes{Code: 0, Message: "已清空该用户的训练与绑定数据"}, nil
}

func NewSpiderService(data *data.Data, spider *task.SpiderTask) *SpiderService {
	// 进程启动清除残留 purge 锁（上次崩溃 / 未 defer 的旧版本）
	if data != nil {
		ClearPurgeLock(data.RDB)
	}
	return &SpiderService{
		db:     data.DB,
		rdb:    data.RDB,
		spider: spider,
	}
}
