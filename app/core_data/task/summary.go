package task

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cwxu-algo/app/common/event"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"github.com/streadway/amqp"
)

const summaryPendingTTL = 20 * time.Minute

type SummaryTask struct {
	mq  *event.RabbitMQ
	rdb *redis.Client
}

func NewSummaryTask(mq *event.RabbitMQ, rdb *redis.Client) *SummaryTask {
	return &SummaryTask{mq: mq, rdb: rdb}
}

func summaryPendingKey(userId int64, typ string) string {
	return fmt.Sprintf("summary:pending:%s:%d", typ, userId)
}

func (t *SummaryTask) Do(userId int64, typ string) {
	if t.mq == nil {
		log.Errorf("SummaryTask: mq not ready")
		return
	}
	if t.rdb != nil {
		ctx := context.Background()
		ok, err := t.rdb.SetNX(ctx, summaryPendingKey(userId, typ), "1", summaryPendingTTL).Result()
		if err == nil && !ok {
			log.Debugf("SummaryTask: dedup skip user=%d type=%s", userId, typ)
			return
		}
	}
	if _, err := t.mq.QueueDeclare("summary", true, false, false, false, nil); err != nil {
		log.Errorf("SummaryTask: QueueDeclare failed: %v", err)
		return
	}
	e := event.SummaryEvent{UserId: userId, Type: typ}
	body, err := json.Marshal(e)
	if err != nil {
		log.Errorf("SummaryTask: json.Marshal failed: %v", err)
		return
	}
	if err := t.mq.Publish("", "summary", false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	}); err != nil {
		log.Errorf("SummaryTask: Publish failed: %v", err)
	}
}
