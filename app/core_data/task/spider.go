package task

import (
	"cwxu-algo/app/common/event"
	"encoding/json"

	"github.com/go-kratos/kratos/v2/log"
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
	q, err := t.rabbitMQ.QueueDeclare("spider", true, false, false, false, nil)
	if err != nil {
		log.Errorf("SpiderTask: QueueDeclare failed: %v", err)
		return
	}
	e := event.SpiderEvent{UserId: userId, NeedAll: needAll}
	body, err := json.Marshal(e)
	if err != nil {
		log.Errorf("SpiderTask: json.Marshal failed: %v", err)
		return
	}
	if err := t.rabbitMQ.Publish("", q.Name, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	}); err != nil {
		log.Errorf("SpiderTask: Publish failed: %v", err)
	}
}
