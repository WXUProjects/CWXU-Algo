package event

import (
	"cwxu-algo/app/common/conf"
	"fmt"
	"sync"

	"github.com/google/wire"
	"github.com/streadway/amqp"
)

// RabbitMQ 连接封装。
// amqp.Channel 非并发安全：消费/发布必须使用独立 Channel；
// 发布侧通过 Publish 统一加锁，避免多 goroutine 共用一个 Ch 写坏连接。
type RabbitMQ struct {
	Conn *amqp.Connection

	pubMu sync.Mutex
	pubCh *amqp.Channel
}

func NewRabbitMQ(data *conf.Server) (*RabbitMQ, func(), error) {
	conn, err := amqp.Dial(data.AmqpDsn)
	if err != nil {
		return nil, func() {}, err
	}
	pubCh, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, func() {}, err
	}
	mq := &RabbitMQ{
		Conn:  conn,
		pubCh: pubCh,
	}
	return mq, func() {
		mq.pubMu.Lock()
		if mq.pubCh != nil {
			_ = mq.pubCh.Close()
			mq.pubCh = nil
		}
		mq.pubMu.Unlock()
		_ = conn.Close()
	}, nil
}

// OpenChannel 打开新 Channel（供消费者专用，勿与 Publish 共用）
func (r *RabbitMQ) OpenChannel() (*amqp.Channel, error) {
	if r == nil || r.Conn == nil {
		return nil, fmt.Errorf("mq not ready")
	}
	return r.Conn.Channel()
}

// ensurePubCh 确保发布 channel 可用（连接断后重建）
func (r *RabbitMQ) ensurePubCh() error {
	if r == nil || r.Conn == nil {
		return fmt.Errorf("mq not ready")
	}
	if r.pubCh != nil {
		return nil
	}
	ch, err := r.Conn.Channel()
	if err != nil {
		return err
	}
	r.pubCh = ch
	return nil
}

// Publish 线程安全发布
func (r *RabbitMQ) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if r == nil {
		return fmt.Errorf("mq not ready")
	}
	r.pubMu.Lock()
	defer r.pubMu.Unlock()
	if err := r.ensurePubCh(); err != nil {
		return err
	}
	err := r.pubCh.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		// channel 可能已死，下次重建
		_ = r.pubCh.Close()
		r.pubCh = nil
		return err
	}
	return nil
}

// QueueDeclare 线程安全声明队列（发布前）
func (r *RabbitMQ) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if r == nil {
		return amqp.Queue{}, fmt.Errorf("mq not ready")
	}
	r.pubMu.Lock()
	defer r.pubMu.Unlock()
	if err := r.ensurePubCh(); err != nil {
		return amqp.Queue{}, err
	}
	q, err := r.pubCh.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		_ = r.pubCh.Close()
		r.pubCh = nil
		return amqp.Queue{}, err
	}
	return q, nil
}

// QueuePurge 线程安全清空队列
func (r *RabbitMQ) QueuePurge(name string, noWait bool) (int, error) {
	if r == nil {
		return 0, fmt.Errorf("mq not ready")
	}
	r.pubMu.Lock()
	defer r.pubMu.Unlock()
	if err := r.ensurePubCh(); err != nil {
		return 0, err
	}
	n, err := r.pubCh.QueuePurge(name, noWait)
	if err != nil {
		_ = r.pubCh.Close()
		r.pubCh = nil
		return 0, err
	}
	return n, nil
}

// QueueInspect 用临时 channel 被动查询队列（消息数/消费者数），不干扰消费
func (r *RabbitMQ) QueueInspect(name string) (amqp.Queue, error) {
	if r == nil || r.Conn == nil {
		return amqp.Queue{}, fmt.Errorf("mq not ready")
	}
	ch, err := r.Conn.Channel()
	if err != nil {
		return amqp.Queue{}, err
	}
	defer ch.Close()
	return ch.QueueInspect(name)
}

// Ch 兼容旧代码：返回发布 channel（已不推荐直接使用）
func (r *RabbitMQ) Ch() *amqp.Channel {
	if r == nil {
		return nil
	}
	r.pubMu.Lock()
	defer r.pubMu.Unlock()
	_ = r.ensurePubCh()
	return r.pubCh
}

var ProviderSet = wire.NewSet(NewRabbitMQ)
