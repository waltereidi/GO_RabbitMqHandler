package model

import (
	"go_rabbitmqhandler/internal/service/consumer"
)

type RabbitMQHandlerConfig struct {
	Host            string
	Port            int
	Username        string
	Password        string
	Queue           string
	AbstractFactory consumer.FactoryHandler
}
