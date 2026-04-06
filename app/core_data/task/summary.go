package task

import (
	"cwxu-algo/app/common/event"
	"encoding/json"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
)

type SummaryTask struct {
	rabbitMQ *amqp.Channel
}

func NewSummaryTask(rabbitMQ *event.RabbitMQ) *SummaryTask {
	return &SummaryTask{
		rabbitMQ: rabbitMQ.Ch,
	}
}

func (t *SummaryTask) Do(userId int64, typ string) {
	q, err := t.rabbitMQ.QueueDeclare("summary", true, false, false, false, nil)
	if err != nil {
		log.Errorf("SummaryTask: QueueDeclare failed: %v", err)
		return
	}
	e := event.SummaryEvent{UserId: userId, Type: typ}
	body, err := json.Marshal(e)
	if err != nil {
		log.Errorf("SummaryTask: json.Marshal failed: %v", err)
		return
	}
	if err := t.rabbitMQ.Publish("", q.Name, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	}); err != nil {
		log.Errorf("SummaryTask: Publish failed: %v", err)
	}
}
