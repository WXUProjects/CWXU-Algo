package event

import (
	"encoding/json"
	"testing"

	"github.com/streadway/amqp"
)

// TestNewRabbitMQ 发布者 Publisher
func TestNewRabbitMQ(t *testing.T) {
	conn, err := amqp.Dial("amqp://cwxu-algo:cwxu-algo@192.168.1.7:5672/algo")
	if err != nil {
		t.Error(err)
	}
	ch, _ := conn.Channel()
	defer ch.Close()
	q, _ := ch.QueueDeclare("spider", true, false, false, false, nil)
	e := SpiderEvent{UserId: 1, NeedAll: true}
	body, _ := json.Marshal(e)
	_ = ch.Publish("", q.Name, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})

}
