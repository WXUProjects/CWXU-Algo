package event

import (
	"cwxu-algo/app/common/conf"

	"github.com/google/wire"
	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	Ch   *amqp.Channel
	Conn *amqp.Connection
}

func NewRabbitMQ(data *conf.Server) (*RabbitMQ, func(), error) {
	conn, err := amqp.Dial(data.AmqpDsn)
	if err != nil {
		return nil, func() {}, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, func() {}, err
	}
	return &RabbitMQ{
			Ch:   ch,
			Conn: conn,
		}, func() {
			_ = ch.Close()
			_ = conn.Close()
		}, nil
}

var ProviderSet = wire.NewSet(NewRabbitMQ)
