package service

import (
	"context"
	"cwxu-algo/app/common/event"
	"cwxu-algo/app/common/utils/mqconsume"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
)

const (
	// 默认各 4；可用 CWXU_PROBLEM_FETCH_CONCURRENCY / CWXU_PROBLEM_ANALYZE_CONCURRENCY 覆盖
	defaultProblemFetchConcurrency   = 4
	defaultProblemAnalyzeConcurrency = 4
	problemMaxRetry                  = 5
)

// 进程内解析一次，供 consumer 与 progress 面板共用
var (
	problemFetchConcurrency   = mqconsume.ConcurrencyFromEnv("CWXU_PROBLEM_FETCH_CONCURRENCY", defaultProblemFetchConcurrency)
	problemAnalyzeConcurrency = mqconsume.ConcurrencyFromEnv("CWXU_PROBLEM_ANALYZE_CONCURRENCY", defaultProblemAnalyzeConcurrency)
)

func retryCount(h amqp.Table) int {
	if h == nil {
		return 0
	}
	v, ok := h[mqconsume.RetryHeader]
	if !ok || v == nil {
		return 0
	}
	switch t := v.(type) {
	case int:
		return t
	case int32:
		return int(t)
	case int64:
		return int(t)
	case float64:
		return int(t)
	case string:
		n, _ := strconv.Atoi(t)
		return n
	case []byte:
		n, _ := strconv.Atoi(string(t))
		return n
	default:
		return 0
	}
}

func requeueWithRetry(mq *event.RabbitMQ, queue string, d amqp.Delivery, max int) {
	n := retryCount(d.Headers)
	if n >= max {
		log.Errorf("queue=%s drop after %d retries", queue, n)
		_ = d.Nack(false, false)
		return
	}
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers[mqconsume.RetryHeader] = n + 1
	if err := mq.Publish("", queue, false, false, amqp.Publishing{
		ContentType:  d.ContentType,
		Body:         d.Body,
		DeliveryMode: amqp.Persistent,
		Headers:      headers,
	}); err != nil {
		log.Errorf("queue=%s requeue publish failed: %v", queue, err)
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}

// ProblemFetchConsumer 消费 problem_fetch：仅爬取
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
	log.Infof("problem_fetch consumer 循环启动")
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
	// 队列由发布侧创建；此处禁止 QueueDeclare（args 不一致会 PRECONDITION 杀 channel）
	ch, err := c.mq.OpenChannel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Qos(problemFetchConcurrency, 0, false); err != nil {
		return err
	}
	// consumer tag 留空，避免多实例/重连 tag 冲突
	msgs, err := ch.Consume("problem_fetch", "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	log.Infof("problem_fetch consumer 已就绪 concurrency=%d queue=problem_fetch", problemFetchConcurrency)

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
				log.Warnf("problem_fetch id=%d requeue: fetch paused", msg.ProblemID)
				time.Sleep(2 * time.Second)
				_ = d.Nack(false, true)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := c.problem.ProcessFetch(ctx, msg); err != nil {
				if strings.Contains(err.Error(), "paused") {
					log.Warnf("RabbitMQ(problem_fetch) id=%d requeue paused: %v", msg.ProblemID, err)
					time.Sleep(2 * time.Second)
					_ = d.Nack(false, true)
					return
				}
				log.Errorf("RabbitMQ(problem_fetch) id=%d: %v", msg.ProblemID, err)
				requeueWithRetry(c.mq, "problem_fetch", d, problemMaxRetry)
				return
			}
			_ = d.Ack(false)
		}(d)
	}
	wg.Wait()
	return nil
}

// ProblemAnalyzeConsumer 消费 problem_analyze：仅 AI
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
	log.Infof("problem_analyze consumer 循环启动")
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
	// 队列由发布侧创建；此处禁止 QueueDeclare（args 不一致会 PRECONDITION 杀 channel）
	ch, err := c.mq.OpenChannel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Qos(problemAnalyzeConcurrency, 0, false); err != nil {
		return err
	}
	msgs, err := ch.Consume("problem_analyze", "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	log.Infof("problem_analyze consumer 已就绪 concurrency=%d queue=problem_analyze", problemAnalyzeConcurrency)

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
				log.Warnf("problem_analyze id=%d requeue: AI paused", msg.ProblemID)
				time.Sleep(2 * time.Second)
				_ = d.Nack(false, true)
				return
			}
			// 流式 AI：整体上限 10 分钟，避免 worker 永久占用
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			err := c.problem.ProcessAnalyze(ctx, msg)
			cancel()
			if err != nil {
				if strings.Contains(err.Error(), "paused") {
					log.Warnf("RabbitMQ(problem_analyze) id=%d requeue paused: %v", msg.ProblemID, err)
					time.Sleep(2 * time.Second)
					_ = d.Nack(false, true)
					return
				}
				log.Errorf("RabbitMQ(problem_analyze) id=%d: %v", msg.ProblemID, err)
				requeueWithRetry(c.mq, "problem_analyze", d, problemMaxRetry)
				return
			}
			_ = d.Ack(false)
		}(d)
	}
	wg.Wait()
	return nil
}
