package interfaces

import (
	"github.com/streadway/amqp"
)

type Consumer interface {
	Consume() ([]byte, error)
	SetQueue(queueName string)
	ConfigureConsumer(ch *amqp.Channel, config models.ConsumerConfig) error
}
