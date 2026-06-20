package consumer

import (
	"fmt"
	"strings"
)

type ConsumerConfig struct {
	QueueName       string
	Durable         bool
	AutoDelete      bool
	Exclusive       bool
	NoWait          bool
	Args            map[string]interface{}
	AbstractFactory FactoryHandler
}

func NewConsumerConfig_test() ConsumerConfig {
	return ConsumerConfig{
		QueueName:       "test_queue",
		Durable:         true,
		AutoDelete:      false,
		Exclusive:       false,
		NoWait:          false,
		Args:            nil,
		AbstractFactory: nil,
	}
}
func (sAf *ConsumerConfig) SetAbstractFactory(factory FactoryHandler) {

}

func (nCC *ConsumerConfig) NewConsumerConfig(queueName string,
	durable bool,
	autoDelete bool,
	exclusive bool,
	noWait bool,
	args map[string]interface{},
	abstractFactory FactoryHandler) error {

	result := ConsumerConfig{
		QueueName:       queueName,
		Durable:         durable,
		AutoDelete:      autoDelete,
		Exclusive:       exclusive,
		NoWait:          noWait,
		Args:            args,
		AbstractFactory: abstractFactory,
	}

	isValid := nCC.isValid(result)

	if isValid != nil {
		return isValid
	}
	return nil
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
