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
		// 必须捕获 d 副本，避免并发下 delivery 变量复用导致 Ack/Nack 错乱
		go func(d amqp.Delivery) {
			msg := event.SummaryEvent{}
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Errorf("RabbitMQ(Summary): 解析json出错 %s", err.Error())
				_ = d.Nack(false, false)
				return
			}
			var runErr error
			switch msg.Type {
			case "PersonalLastDay":
				runErr = c.summary.PersonalLastDay(msg.UserId)
			case "PersonalRecent":
				runErr = c.summary.PersonalRecent(msg.UserId)
			default:
				log.Errorf("RabbitMQ(Summary): 未知类型 %s", msg.Type)
				_ = d.Nack(false, false)
				return
			}
			if runErr != nil {
				log.Errorf("RabbitMQ(Summary) user=%d type=%s: %v", msg.UserId, msg.Type, runErr)
				// 短暂失败允许重入队
				_ = d.Nack(false, true)
				return
			}
			_ = d.Ack(false)
		}(d)
	}
}
