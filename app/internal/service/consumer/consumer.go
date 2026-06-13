package consumer

import (
	"github.com/streadway/amqp"
)

type Consumer interface {
	Consume(ch *amqp.Channel)
	SetQueue(queueName string)
	ConfigureConsumer(config ConsumerConfig) error
}

