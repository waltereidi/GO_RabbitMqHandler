package model

import (
	"fmt"
	"go_rabbitmqhandler/internal/interfaces"
	"strings"
)

type ConsumerConfig struct {
	QueueName       string
	Durable         bool
	AutoDelete      bool
	Exclusive       bool
	NoWait          bool
	Args            map[string]interface{}
	AbstractFactory interfaces.AbstractFactoryHandler
}

func NewConsumerConfig(queueName string,
	durable bool,
	autoDelete bool,
	exclusive bool,
	noWait bool,
	args map[string]interface{},
	abstractFactory interfaces.AbstractFactoryHandler) error {

	result = ConsumerConfig{
		QueueName:       queueName,
		Durable:         durable,
		AutoDelete:      autoDelete,
		Exclusive:       exclusive,
		NoWait:          noWait,
		Args:            args,
		AbstractFactory: abstractFactory,
	}
	
	isValid := isValid(result)

	if isValid != nil {
		return nil, isValid
	}
}

func (iV *ConsumerConfig) isValid(config ConsumerConfig) error {
	if iV.isEmpty(config.QueueName) {
		return fmt.Errorf("queue name is empty")
	}
	if iV.AbstractFactory == nil {
		return fmt.Errorf("abstract factory is nil")
	}
	return nil
}

func (e *ConsumerConfig) isEmpty(s string) bool {
	return strings.TrimSpace(string(s)) == ""
}
