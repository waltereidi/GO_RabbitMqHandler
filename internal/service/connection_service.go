package service

import (
	"fmt"
	"go_rabbitmqhandler/internal/interfaces"
	"log"

	"github.com/go-playground/locales/lo"
	"github.com/samber/lo"
	"github.com/streadway/amqp"
)

type RabbitMQConfigComposite struct {
	channels   []ChannelConfig
	connection *amqp.Connection
}
type ChannelConfig struct {
	publishers []interfaces.Publisher
	consumers  []interfaces.Consumer
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

func (rmc *RabbitMQConfigComposite) GetChannel(channelName string) bool {
	channelNames := lo.pluck(rmc.channels, func(c ChannelConfig) string {
		return c.name
	})
	return lo.Contains(channelNames, channelName)
}


func (rmc *RabbitMQConfigComposite) AddConsumer(channel string, queueName string) {

}

func (rmc *RabbitMQConfigComposite) AddPublisher(queueName string) {

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
