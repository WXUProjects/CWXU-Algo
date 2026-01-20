package task

import (
	"cwxu-algo/app/common/event"
	"encoding/json"

	"github.com/streadway/amqp"
)

type SpiderTask struct {
	rabbitMQ *amqp.Channel
}

func NewSpiderTask(rabbitMQ *event.RabbitMQ) *SpiderTask {
	return &SpiderTask{
		rabbitMQ: rabbitMQ.Ch,
	}
}

func (t *SpiderTask) Do(userId int64, needAll bool) {
	q, _ := t.rabbitMQ.QueueDeclare("spider", true, false, false, false, nil)
	e := event.SpiderEvent{UserId: userId, NeedAll: needAll}
	body, _ := json.Marshal(e)
	_ = t.rabbitMQ.Publish("", q.Name, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}
