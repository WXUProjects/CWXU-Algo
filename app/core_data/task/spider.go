package task

import (
	"context"
	"encoding/json"
	"fmt"
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
}

func NewSpiderTask(mq *event.RabbitMQ, rdb *redis.Client) *SpiderTask {
	return &SpiderTask{mq: mq, rdb: rdb}
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
	if _, err := t.mq.QueueDeclare("spider", true, false, false, false, nil); err != nil {
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

// DoBatch 分批入队，避免 UpdateAll 瞬时打满队列（功能仍是「全部触发」）。
// ctx 取消时提前结束（进程停机）。
func (t *SpiderTask) DoBatch(ctx context.Context, userIds []int64, needAll bool, batchSize int, interval time.Duration) {
	if batchSize <= 0 {
		batchSize = 20
	}
	if interval <= 0 {
		interval = time.Minute
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
