package service

import (
	"cwxu-algo/app/common/event"
	"encoding/json"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
)

type Consumer struct {
	ch     *amqp.Channel
	spider *SpiderUseCase
}

func NewConsumer(ch *event.RabbitMQ, spider *SpiderUseCase) *Consumer {
	return &Consumer{
		ch:     ch.Ch,
		spider: spider,
	}
}

func (c *Consumer) Consume() {
	q, err := c.ch.QueueDeclare("spider", true, false, false, false, nil)
	if err != nil {
		log.Errorf("打开消息队列 Spider 失败: %v", err)
		return
	}
	_ = c.ch.Qos(2, 0, false)
	msgs, err := c.ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Errorf("打开消息队列 消息 失败: %v", err)
		return
	}
	for d := range msgs {
		go func(d amqp.Delivery) {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("RabbitMQ(Spider): panic recovered: %v", r)
					_ = d.Nack(false, false)
				}
			}()
			msg := event.SpiderEvent{}
			err := json.Unmarshal(d.Body, &msg)
			if err != nil {
				log.Errorf("RabbitMQ(Spider): 解析json出错 %s", err.Error())
				_ = d.Nack(false, false)
				return
			}
			err = c.spider.LoadData(msg.UserId, msg.NeedAll)
			if err != nil {
				log.Errorf("RabbitMQ(Spider): %v", err)
				_ = d.Nack(false, false)
				return
			}
			_ = d.Ack(false)
		}(d)
	}
}
