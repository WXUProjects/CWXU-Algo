package service

import (
	"cwxu-algo/app/common/event"
	"encoding/json"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
)

type Consumer struct {
	mq     *event.RabbitMQ
	spider *SpiderUseCase
}

func NewConsumer(mq *event.RabbitMQ, spider *SpiderUseCase) *Consumer {
	return &Consumer{
		mq:     mq,
		spider: spider,
	}
}

func (c *Consumer) Consume() {
	for {
		if err := c.consumeOnce(); err != nil {
			log.Errorf("spider consumer 退出: %v，5s 后重连", err)
		} else {
			log.Warnf("spider consumer 通道关闭，5s 后重连")
		}
		time.Sleep(5 * time.Second)
	}
}

func (c *Consumer) consumeOnce() error {
	ch, err := c.mq.OpenChannel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("spider", true, false, false, false, nil)
	if err != nil {
		return err
	}
	if err := ch.Qos(2, 0, false); err != nil {
		return err
	}
	msgs, err := ch.Consume(q.Name, "core-data-spider", false, false, false, false, nil)
	if err != nil {
		return err
	}
	log.Infof("spider consumer 已就绪")

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
	return nil
}
