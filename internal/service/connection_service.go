package service

import (
	"fmt"
	"go_rabbitmqhandler/internal/interfaces"
	"log"

	"github.com/streadway/amqp"
)

type RabbitMQConfigComposite[T any] struct {
	channels   ChannelConfig[T]
	connection *amqp.Connection
}
type ChannelConfig[T any] struct {
	publishers []interfaces.Publisher
	consumers  []interfaces.Consumer[T]
	channel    *amqp.Channel
	name       string
}

func FindOrElse[T any](
	items []T,
	predicate func(T) bool,
	orElse func() T,
) T {
	for _, item := range items {
		if predicate(item) {
			return item
		}
	}

	return orElse()
}

func (rmc *RabbitMQConfigComposite[T]) GetChannel(channelName string) ChannelConfig[T] {

	channel := FindOrElse(
		rmc.channels.consumers ,
		func(p ChannelConfig[T]) bool {
			return p.name == channelName
		},
		nil,
	)

	return channel
}

func (rmc *RabbitMQConfigComposite[T]) AddConsumer(channelName string,
	queueName string,
	abstractFactory interfaces.AbstractFactoryHandler,
	consumer interfaces.Consumer[T] ){

	channel := rmc.GetChannel(channelName)

	channel.consumers = append(channel.consumers, abstractFactory )

}

func (rmc *RabbitMQConfigComposite[T]) AddPublisher(queueName string) {

}
func AddChannel(channelName string) Option {
	return func(rmc *RabbitMQConfigComposite) error {

		channel, err := rmc.connection.Channel("default")

		return nil
	}
}

func (c *RabbitMQConfigComposite) Configure(opts ...Option) (*RabbitMQConfigComposite, []error) {
	s := &RabbitMQConfigComposite{}
	errors := []error{}

	for _, opt := range opts {
		opt(s)

	}
	return s, errors
}

func ConfigureConnection(host string, port string, un string, pwd string) Option {
	return func(rmc *RabbitMQConfigComposite) error {

		conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/", un, pwd, host, port))
		if err != nil {
			return err
		}
		rmc.connection = conn

		return nil
	}
}

func (rmc *RabbitMQConfigComposite) CloseConnection() {
	defer rmc.connection.Close()
}
func (rmc *RabbitMQConfigComposite) CloseChannel(channelName string) {

}

func (FoE *RabbitMQConfigComposite) failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		//FoE.errors = append(FoE.errors, err)
	}
}
