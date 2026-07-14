package service

import (
	"context"
	"cwxu-algo/app/common/event"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
)

const (
	// 题面爬取并发
	problemFetchConcurrency = 2
	// AI 分析并发
	problemAnalyzeConcurrency = 3
)

// ProblemFetchConsumer 消费 problem_fetch：仅爬取，并发 2
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
	_ = c.ch.Qos(problemFetchConcurrency, 0, false)
	msgs, err := c.ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Errorf("消费 problem_fetch 失败: %v", err)
		return
	}
	sem := make(chan struct{}, problemFetchConcurrency)
	var wg sync.WaitGroup
	for d := range msgs {
		sem <- struct{}{}
		wg.Add(1)
		go func(d amqp.Delivery) {
			defer wg.Done()
			defer func() { <-sem }()
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
			// 题面爬取不因 AI 紧急停止而中断
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := c.problem.ProcessFetch(ctx, msg); err != nil {
				log.Errorf("RabbitMQ(problem_fetch) id=%d: %v", msg.ProblemID, err)
				_ = d.Nack(false, false)
				return
			}
			_ = d.Ack(false)
		}(d)
	}
	wg.Wait()
}

// ProblemAnalyzeConsumer 消费 problem_analyze：仅 AI，并发 3
type ProblemAnalyzeConsumer struct {
	ch      *amqp.Channel
	problem *ProblemUseCase
}

func NewProblemAnalyzeConsumer(mq *event.RabbitMQ, problem *ProblemUseCase) *ProblemAnalyzeConsumer {
	return &ProblemAnalyzeConsumer{
		ch:      mq.Ch,
		problem: problem,
	}
}

func (c *ProblemAnalyzeConsumer) Consume() {
	q, err := c.ch.QueueDeclare("problem_analyze", true, false, false, false, nil)
	if err != nil {
		log.Errorf("打开消息队列 problem_analyze 失败: %v", err)
		return
	}
	_ = c.ch.Qos(problemAnalyzeConcurrency, 0, false)
	msgs, err := c.ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Errorf("消费 problem_analyze 失败: %v", err)
		return
	}
	sem := make(chan struct{}, problemAnalyzeConcurrency)
	var wg sync.WaitGroup
	for d := range msgs {
		sem <- struct{}{}
		wg.Add(1)
		go func(d amqp.Delivery) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("RabbitMQ(problem_analyze): panic: %v", r)
					_ = d.Nack(false, false)
				}
			}()
			var msg event.ProblemAnalyzeEvent
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Errorf("RabbitMQ(problem_analyze): json %v", err)
				_ = d.Nack(false, false)
				return
			}
			if pipelineControl.IsAnalyzePaused() {
				// AI 紧急停止：丢弃分析消息（队列可能已 purge）
				_ = d.Ack(false)
				return
			}
			// AI 分析超时 240s
			ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
			defer cancel()
			if err := c.problem.ProcessAnalyze(ctx, msg); err != nil {
				log.Errorf("RabbitMQ(problem_analyze) id=%d: %v", msg.ProblemID, err)
				_ = d.Nack(false, false)
				return
			}
			_ = d.Ack(false)
		}(d)
	}
	wg.Wait()
}
