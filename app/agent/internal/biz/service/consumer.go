package service

import (
	"cwxu-algo/app/common/event"
	"encoding/json"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
)

type Consumer struct {
	mq      *event.RabbitMQ
	summary *SummaryUseCase
}

func NewConsumer(mq *event.RabbitMQ, summary *SummaryUseCase) *Consumer {
	return &Consumer{
		mq:      mq,
		summary: summary,
	}
}

func (c *Consumer) Consume() {
	for {
		if err := c.consumeOnce(); err != nil {
			log.Errorf("summary consumer 退出: %v，5s 后重连", err)
		} else {
			log.Warnf("summary consumer 通道关闭，5s 后重连")
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

	q, err := ch.QueueDeclare("summary", true, false, false, false, nil)
	if err != nil {
		return err
	}
	if err := ch.Qos(2, 0, false); err != nil {
		return err
	}
	msgs, err := ch.Consume(q.Name, "agent-summary", false, false, false, false, nil)
	if err != nil {
		return err
	}
	log.Infof("summary consumer 已就绪")

	for d := range msgs {
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
				_ = d.Nack(false, true)
				return
			}
			_ = d.Ack(false)
		}(d)
	}
	return nil
}
