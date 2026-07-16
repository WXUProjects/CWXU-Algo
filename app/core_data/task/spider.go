package task

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"cwxu-algo/app/common/event"
	"cwxu-algo/app/core_data/internal/spidermetrics"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"github.com/streadway/amqp"
)

const (
	// pendingTTL 待处理去重窗口：同用户在此时间内重复入队会被跳过
	pendingTTL = 15 * time.Minute
	// inflightTTL 执行中标记，防止重复消费叠跑
	inflightTTL = 45 * time.Minute
)

type SpiderTask struct {
	mq  *event.RabbitMQ
	rdb *redis.Client
	// queueReady 避免每次入队都 QueueDeclare（50 用户 cron 高频时省 RTT）
	queueReady atomic.Bool
}

func NewSpiderTask(mq *event.RabbitMQ, rdb *redis.Client) *SpiderTask {
	return &SpiderTask{mq: mq, rdb: rdb}
}

func (t *SpiderTask) ensureSpiderQueue() error {
	if t.queueReady.Load() {
		return nil
	}
	if _, err := t.mq.QueueDeclare("spider", true, false, false, false, nil); err != nil {
		return err
	}
	t.queueReady.Store(true)
	return nil
}

func pendingKey(userId int64, platform string) string {
	if platform == "" {
		return fmt.Sprintf("spider:pending:%d", userId)
	}
	return fmt.Sprintf("spider:pending:%d:%s", userId, platform)
}

func InflightKey(userId int64, platform string) string {
	if platform == "" {
		return fmt.Sprintf("spider:inflight:%d", userId)
	}
	return fmt.Sprintf("spider:inflight:%d:%s", userId, platform)
}

// Do 入队全平台爬虫任务。同 user 全量任务在 pending/inflight 窗口内去重。
func (t *SpiderTask) Do(userId int64, needAll bool) {
	t.DoPlatform(userId, "", needAll)
}

// DoPlatform 入队爬虫任务；platform 非空时只抓该平台。
// 去重：inflight 存在则跳过；pending 用 SetNX 原子占坑（多实例/并发安全）。
func (t *SpiderTask) DoPlatform(userId int64, platform string, needAll bool) {
	if t.mq == nil {
		log.Errorf("SpiderTask: mq not ready")
		return
	}
	pk := pendingKey(userId, platform)
	if t.rdb != nil {
		ctx := context.Background()
		// 正在执行：不重复入队
		if n, err := t.rdb.Exists(ctx, InflightKey(userId, platform)).Result(); err == nil && n > 0 {
			spidermetrics.IncDedupSkipped()
			log.Debugf("SpiderTask: dedup skip inflight user=%d platform=%q needAll=%v", userId, platform, needAll)
			return
		}
		// 原子占 pending；Exists+Set 有竞态，多副本会各塞一条
		ok, err := t.rdb.SetNX(ctx, pk, "1", pendingTTL).Result()
		if err != nil {
			log.Warnf("SpiderTask: setnx pending failed (allow): %v", err)
		} else if !ok {
			spidermetrics.IncDedupSkipped()
			log.Debugf("SpiderTask: dedup skip pending user=%d platform=%q needAll=%v", userId, platform, needAll)
			return
		}
	}
	if err := t.ensureSpiderQueue(); err != nil {
		log.Errorf("SpiderTask: QueueDeclare failed: %v", err)
		t.clearPending(userId, platform)
		return
	}
	e := event.SpiderEvent{UserId: userId, NeedAll: needAll, Platform: platform}
	body, err := json.Marshal(e)
	if err != nil {
		log.Errorf("SpiderTask: json.Marshal failed: %v", err)
		t.clearPending(userId, platform)
		return
	}
	if err := t.mq.Publish("", "spider", false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	}); err != nil {
		log.Errorf("SpiderTask: Publish failed: %v", err)
		// 连接可能已重置，下次重新 declare
		t.queueReady.Store(false)
		t.clearPending(userId, platform)
		return
	}
	spidermetrics.IncEnqueued()
}

func (t *SpiderTask) clearPending(userId int64, platform string) {
	if t.rdb == nil {
		return
	}
	_ = t.rdb.Del(context.Background(), pendingKey(userId, platform)).Err()
}

// MarkInflight 消费开始时调用
func (t *SpiderTask) MarkInflight(userId int64, platform string) {
	if t.rdb == nil {
		return
	}
	ctx := context.Background()
	_ = t.rdb.Del(ctx, pendingKey(userId, platform)).Err()
	_ = t.rdb.Set(ctx, InflightKey(userId, platform), "1", inflightTTL).Err()
}

// ClearInflight 消费结束时调用
func (t *SpiderTask) ClearInflight(userId int64, platform string) {
	if t.rdb == nil {
		return
	}
	_ = t.rdb.Del(context.Background(), InflightKey(userId, platform)).Err()
}

// ResetDedup 清除 pending/inflight，强制允许再次入队。
// 重绑 OJ 时调用：旧任务可能仍占着 pending/inflight，否则 DoPlatform 会被静默跳过，
// 用户已删旧明细却再也等不到新全量同步。
func (t *SpiderTask) ResetDedup(userId int64, platform string) {
	if t.rdb == nil {
		return
	}
	ctx := context.Background()
	_ = t.rdb.Del(ctx, pendingKey(userId, platform), InflightKey(userId, platform)).Err()
}

// BumpGeneration 递增 user+platform 爬取代数。重绑后旧任务写入前应校验代数，避免把已删数据写回。
func (t *SpiderTask) BumpGeneration(userId int64, platform string) int64 {
	if t.rdb == nil || platform == "" {
		return 0
	}
	n, err := t.rdb.Incr(context.Background(), GenerationKey(userId, platform)).Result()
	if err != nil {
		log.Warnf("SpiderTask: bump generation user=%d platform=%s: %v", userId, platform, err)
		return 0
	}
	// 避免 key 永不过期膨胀；绑定活跃用户会持续刷新
	_ = t.rdb.Expire(context.Background(), GenerationKey(userId, platform), 7*24*time.Hour).Err()
	return n
}

// GenerationKey 爬取代数 Redis key
func GenerationKey(userId int64, platform string) string {
	return fmt.Sprintf("spider:gen:%d:%s", userId, platform)
}

// CurrentGeneration 读取当前代数（无 key 视为 0）
func CurrentGeneration(rdb *redis.Client, userId int64, platform string) int64 {
	if rdb == nil || platform == "" {
		return 0
	}
	v, err := rdb.Get(context.Background(), GenerationKey(userId, platform)).Int64()
	if err != nil {
		return 0
	}
	return v
}

// DoBatch 分批入队，避免 UpdateAll 瞬时打满队列（功能仍是「全部触发」）。
// ctx 取消时提前结束（进程停机）。
// 1w 日活 / 2c4g 默认：10 人一批、间隔 2 分钟，削峰保护单机。
func (t *SpiderTask) DoBatch(ctx context.Context, userIds []int64, needAll bool, batchSize int, interval time.Duration) {
	if batchSize <= 0 {
		batchSize = 10
	}
	if interval <= 0 {
		interval = 2 * time.Minute
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for i, uid := range userIds {
		select {
		case <-ctx.Done():
			log.Warnf("SpiderTask: DoBatch cancelled at %d/%d", i, len(userIds))
			return
		default:
		}
		t.Do(uid, needAll)
		if (i+1)%batchSize == 0 && i+1 < len(userIds) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}
}
