package event

import (
	"cwxu-algo/app/common/conf"

	"github.com/google/wire"
	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	Ch *amqp.Channel
}

func NewRabbitMQ(data *conf.Server) (*RabbitMQ, func(), error) {
	conn, err := amqp.Dial(data.AmqpDsn)
	if err != nil {
		return nil, func() {}, err
	}
	ch, err := conn.Channel()
	return &RabbitMQ{
			Ch: ch,
		}, func() {
			_ = ch.Close()
		}, nil
}

var ProviderSet = wire.NewSet(NewRabbitMQ)
