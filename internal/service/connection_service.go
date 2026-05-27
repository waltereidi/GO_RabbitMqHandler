package service

import (
	"fmt"
	"go_rabbitmqhandler/internal/interfaces"
	"log"

	"github.com/streadway/amqp"
)

type RabbitMQConfig struct {
	consumers  []interfaces.Consumer
	channel    []*amqp.Channel
	publishers []interfaces.Publisher
	connection *amqp.Connection
}

type Option func(*RabbitMQConfig) error

func AddConsumer(consumer interfaces.Consumer) Option {
	return func(cfg *RabbitMQConfig) error {
		cfg.consumers = append(cfg.consumers, consumer)

		return nil
	}
}

func (c *RabbitMQConfig) Configure(opts ...Option) (*RabbitMQConfig, []error) {
	s := &RabbitMQConfig{}
	errors := []error{}

	for _, opt := range opts {
		opt(s)

	}
	return s, errors
}

func ConfigureConnection(host string, port string, un string, pwd string) Option {
	return func(rmc *RabbitMQConfig) error {

		conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/", un, pwd, host, port))
		if err != nil {
			return err
		}
		rmc.connection = conn

		return nil
	}
}
func (rmc *RabbitMQConfig) CloseConnection() {
	defer rmc.connection.Close()
}
func (rmc *RabbitMQConfig) CloseChannel(channelName string) {

}

func (FoE *RabbitMQConfig) failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		//FoE.errors = append(FoE.errors, err)
	}
}
