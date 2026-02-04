package task

import (
	"cwxu-algo/app/common/event"
	"encoding/json"

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
	q, _ := t.rabbitMQ.QueueDeclare("summary", true, false, false, false, nil)
	e := event.SummaryEvent{UserId: userId, Type: typ}
	body, _ := json.Marshal(e)
	_ = t.rabbitMQ.Publish("", q.Name, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}
