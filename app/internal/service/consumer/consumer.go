package consumer

import (
	"github.com/streadway/amqp"
)

type Consumer interface {
	Consume(ch *amqp.Channel)
	SetConfiguration(config *ConsumerConfig)
}
