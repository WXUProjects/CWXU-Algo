package event

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/streadway/amqp"
)

// TestNewRabbitMQ 发布者 Publisher
func TestNewRabbitMQ(t *testing.T) {
	dsn := os.Getenv("CWXU_TEST_AMQP_DSN")
	if dsn == "" {
		t.Skip("set CWXU_TEST_AMQP_DSN to run the RabbitMQ integration test")
	}
	conn, err := amqp.Dial(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	q, err := ch.QueueDeclare("spider", true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	e := SpiderEvent{UserId: 1, NeedAll: true}
	body, _ := json.Marshal(e)
	if err := ch.Publish("", q.Name, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	}); err != nil {
		t.Fatal(err)
	}

}
