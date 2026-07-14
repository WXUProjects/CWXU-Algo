package task

import (
	"cwxu-algo/app/common/event"
	"encoding/json"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
)

type SummaryTask struct {
	mq *event.RabbitMQ
}

func NewSummaryTask(mq *event.RabbitMQ) *SummaryTask {
	return &SummaryTask{mq: mq}
}

func (t *SummaryTask) Do(userId int64, typ string) {
	if t.mq == nil {
		log.Errorf("SummaryTask: mq not ready")
		return
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
