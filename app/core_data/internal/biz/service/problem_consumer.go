package service

import (
	"context"
	"cwxu-algo/app/common/event"
	"encoding/json"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
)

// ProblemFetchConsumer 消费 problem_fetch 队列
type ProblemFetchConsumer struct {
	ch      *amqp.Channel
	problem *ProblemUseCase
}

func NewProblemFetchConsumer(mq *event.RabbitMQ, problem *ProblemUseCase) *ProblemFetchConsumer {
	return &ProblemFetchConsumer{
		ch:      mq.Ch,
		problem: problem,
	}
}

func (c *ProblemFetchConsumer) Consume() {
	q, err := c.ch.QueueDeclare("problem_fetch", true, false, false, false, nil)
	if err != nil {
		log.Errorf("打开消息队列 problem_fetch 失败: %v", err)
		return
	}
	_ = c.ch.Qos(1, 0, false)
	msgs, err := c.ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Errorf("消费 problem_fetch 失败: %v", err)
		return
	}
	for d := range msgs {
		go func(d amqp.Delivery) {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("RabbitMQ(problem_fetch): panic: %v", r)
					_ = d.Nack(false, false)
				}
			}()
			var msg event.ProblemFetchEvent
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Errorf("RabbitMQ(problem_fetch): json %v", err)
				_ = d.Nack(false, false)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			if err := c.problem.ProcessFetch(ctx, msg); err != nil {
				log.Errorf("RabbitMQ(problem_fetch) id=%d: %v", msg.ProblemID, err)
				// 不 requeue 避免毒消息打爆；状态已记 FAILED，可手动 backfill 重试
				_ = d.Nack(false, false)
				return
			}
			_ = d.Ack(false)
		}(d)
	}
}
