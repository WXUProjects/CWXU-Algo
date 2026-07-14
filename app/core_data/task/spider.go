package task

import (
	"cwxu-algo/app/common/event"
	"encoding/json"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
)

type SpiderTask struct {
	mq *event.RabbitMQ
}

func NewSpiderTask(mq *event.RabbitMQ) *SpiderTask {
	return &SpiderTask{mq: mq}
}

func (t *SpiderTask) Do(userId int64, needAll bool) {
	if t.mq == nil {
		log.Errorf("SpiderTask: mq not ready")
		return
	}
	if _, err := t.mq.QueueDeclare("spider", true, false, false, false, nil); err != nil {
		log.Errorf("SpiderTask: QueueDeclare failed: %v", err)
		return
	}
	e := event.SpiderEvent{UserId: userId, NeedAll: needAll}
	body, err := json.Marshal(e)
	if err != nil {
		log.Errorf("SpiderTask: json.Marshal failed: %v", err)
		return
	}
	if err := t.mq.Publish("", "spider", false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	}); err != nil {
		log.Errorf("SpiderTask: Publish failed: %v", err)
	}
}
