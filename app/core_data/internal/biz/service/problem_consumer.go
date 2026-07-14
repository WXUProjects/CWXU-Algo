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
	problemFetchConcurrency = 4
	// AI 分析并发
	problemAnalyzeConcurrency = 8
)

// ProblemFetchConsumer 消费 problem_fetch：仅爬取，并发 4
// 使用独立 Channel + 断线自动重连，避免与发布侧共用 channel 导致消费者静默死亡。
type ProblemFetchConsumer struct {
	mq      *event.RabbitMQ
	problem *ProblemUseCase
}

func NewProblemFetchConsumer(mq *event.RabbitMQ, problem *ProblemUseCase) *ProblemFetchConsumer {
	return &ProblemFetchConsumer{
		mq:      mq,
		problem: problem,
	}
}

func (c *ProblemFetchConsumer) Consume() {
	for {
		if err := c.consumeOnce(); err != nil {
			log.Errorf("problem_fetch consumer 退出: %v，5s 后重连", err)
		} else {
			log.Warnf("problem_fetch consumer 通道关闭，5s 后重连")
		}
		time.Sleep(5 * time.Second)
	}
}

func (c *ProblemFetchConsumer) consumeOnce() error {
	ch, err := c.mq.OpenChannel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("problem_fetch", true, false, false, false, nil)
	if err != nil {
		return err
	}
	if err := ch.Qos(problemFetchConcurrency, 0, false); err != nil {
		return err
	}
	// 独占 consumer tag，便于排查
	msgs, err := ch.Consume(q.Name, "core-data-problem-fetch", false, false, false, false, nil)
	if err != nil {
		return err
	}
	log.Infof("problem_fetch consumer 已就绪 concurrency=%d", problemFetchConcurrency)

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
					_ = d.Nack(false, true)
				}
			}()
			var msg event.ProblemFetchEvent
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Errorf("RabbitMQ(problem_fetch): json %v", err)
				_ = d.Nack(false, false)
				return
			}
			if pipelineControl.IsFetchPaused() {
				// 暂停：丢弃（暂停时已 purge；恢复后用回填/重试再入队）
				_ = d.Ack(false)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := c.problem.ProcessFetch(ctx, msg); err != nil {
				log.Errorf("RabbitMQ(problem_fetch) id=%d: %v", msg.ProblemID, err)
				// 可恢复错误：发回队列重试；永久失败 ProcessFetch 返回 nil 已 Ack
				_ = d.Nack(false, true)
				return
			}
			_ = d.Ack(false)
		}(d)
	}
	wg.Wait()
	return nil
}

// ProblemAnalyzeConsumer 消费 problem_analyze：仅 AI，并发 8
type ProblemAnalyzeConsumer struct {
	mq      *event.RabbitMQ
	problem *ProblemUseCase
}

func NewProblemAnalyzeConsumer(mq *event.RabbitMQ, problem *ProblemUseCase) *ProblemAnalyzeConsumer {
	return &ProblemAnalyzeConsumer{
		mq:      mq,
		problem: problem,
	}
}

func (c *ProblemAnalyzeConsumer) Consume() {
	for {
		if err := c.consumeOnce(); err != nil {
			log.Errorf("problem_analyze consumer 退出: %v，5s 后重连", err)
		} else {
			log.Warnf("problem_analyze consumer 通道关闭，5s 后重连")
		}
		time.Sleep(5 * time.Second)
	}
}

func (c *ProblemAnalyzeConsumer) consumeOnce() error {
	ch, err := c.mq.OpenChannel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("problem_analyze", true, false, false, false, nil)
	if err != nil {
		return err
	}
	if err := ch.Qos(problemAnalyzeConcurrency, 0, false); err != nil {
		return err
	}
	msgs, err := ch.Consume(q.Name, "core-data-problem-analyze", false, false, false, false, nil)
	if err != nil {
		return err
	}
	log.Infof("problem_analyze consumer 已就绪 concurrency=%d", problemAnalyzeConcurrency)

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
					_ = d.Nack(false, true)
				}
			}()
			var msg event.ProblemAnalyzeEvent
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Errorf("RabbitMQ(problem_analyze): json %v", err)
				_ = d.Nack(false, false)
				return
			}
			if pipelineControl.IsAnalyzePaused() {
				// 暂停：丢弃（暂停时已 purge；恢复后用回填/重试再入队）
				_ = d.Ack(false)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
			defer cancel()
			if err := c.problem.ProcessAnalyze(ctx, msg); err != nil {
				log.Errorf("RabbitMQ(problem_analyze) id=%d: %v", msg.ProblemID, err)
				// 分析出错：发回队列重试
				_ = d.Nack(false, true)
				return
			}
			_ = d.Ack(false)
		}(d)
	}
	wg.Wait()
	return nil
}
