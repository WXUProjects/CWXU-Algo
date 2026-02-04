package service

import (
	"cwxu-algo/app/common/event"
	"encoding/json"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
)

type Consumer struct {
	ch      *amqp.Channel
	summary *SummaryUseCase
}

func NewConsumer(ch *event.RabbitMQ, summary *SummaryUseCase) *Consumer {
	return &Consumer{
		ch:      ch.Ch,
		summary: summary,
	}
}

func (c *Consumer) Consume() {
	q, err := c.ch.QueueDeclare("summary", true, false, false, false, nil)
	if err != nil {
		log.Error("打开消息队列 summary 失败", err.Error())
		return
	}
	_ = c.ch.Qos(2, 0, false)
	msgs, err := c.ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Error("打开消息队列 消息 失败")
		return
	}
	for d := range msgs {
		go func() {
			msg := event.SummaryEvent{}
			err := json.Unmarshal(d.Body, &msg)
			if err != nil {
				log.Errorf("RabbitMQ(Summary): 解析json出错 %s", err.Error())
				return
			}
			switch msg.Type {
			case "PersonalLastDay":
				err = c.summary.PersonalLastDay(msg.UserId)
				break
			}
			if err != nil {
				log.Error("RabbitMQ(Summary): " + err.Error())
				return
			}
			_ = d.Ack(false)
		}()

	}
}
