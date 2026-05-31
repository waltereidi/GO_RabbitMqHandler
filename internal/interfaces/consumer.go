package interfaces

import (
	"github.com/streadway/amqp"
)

type Consumer[T any] interface {
	Consume(afh *AbstractFactoryHandler, channel *amqp.Channel) (T, error)
	SetQueue(queueName string)
}
