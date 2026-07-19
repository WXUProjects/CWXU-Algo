package service

import (
	"encoding/json"
	"fmt"
	"sync"

	"cwxu-algo/app/common/event"
	"cwxu-algo/app/common/utils/mqconsume"
	"cwxu-algo/app/core_data/internal/spidermetrics"
	"cwxu-algo/app/core_data/task"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
)

// defaultSpiderConcurrency 默认 4；可用 CWXU_SPIDER_CONCURRENCY 覆盖（1–32）
const defaultSpiderConcurrency = 4

type Consumer struct {
	mq         *event.RabbitMQ
	spider     *SpiderUseCase
	spiderTask *task.SpiderTask
	stopCh     chan struct{}
	stopOnce   sync.Once
}

func NewConsumer(mq *event.RabbitMQ, spider *SpiderUseCase, spiderTask *task.SpiderTask) *Consumer {
	return &Consumer{
		mq:         mq,
		spider:     spider,
		spiderTask: spiderTask,
		stopCh:     make(chan struct{}),
	}
}

func (c *Consumer) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
}

func (c *Consumer) Consume() {
	conc := mqconsume.ConcurrencyFromEnv("CWXU_SPIDER_CONCURRENCY", defaultSpiderConcurrency)
	log.Infof("spider consumer 循环启动 concurrency=%d", conc)
	_ = mqconsume.Run(c.mq, mqconsume.Options{
		Name:             "spider",
		Queue:            "spider",
		Concurrency:      conc,
		MaxRetry:         3,
		DeclareOnMissing: true,
		Stop:             c.stopCh,
		Handler: func(body []byte, _ amqp.Table) error {
			msg := event.SpiderEvent{}
			if err := json.Unmarshal(body, &msg); err != nil {
				log.Errorf("RabbitMQ(Spider): 解析json出错 %s", err.Error())
				// 坏消息：返回 nil 让上层 Ack？不，返回特殊——这里返回 error 会重试；
				// 解析失败应直接丢弃：用不可重试错误由 MaxRetry 后 drop
				return fmt.Errorf("bad json: %w", err)
			}
			if c.spiderTask != nil {
				c.spiderTask.MarkInflight(msg.UserId, msg.Platform)
				defer c.spiderTask.ClearInflight(msg.UserId, msg.Platform)
			}
			start := spidermetrics.RecordStart(msg.NeedAll)
			err := c.spider.LoadData(msg.UserId, msg.NeedAll, msg.Platform)
			spidermetrics.RecordEnd(start, err)
			if err != nil {
				log.Errorf("RabbitMQ(Spider): %v", err)
				return err
			}
			// 任一平台抓取成功即更新「上次同步」时间（个人资料页展示）
			if c.spiderTask != nil {
				c.spiderTask.MarkLastOK(msg.UserId)
			}
			return nil
		},
	})
}
