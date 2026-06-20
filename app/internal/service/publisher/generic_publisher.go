package publisher

import (
	"errors"

	amqp "github.com/streadway/amqp"
)

type GenericPublisher struct {
	queueName string
	channel   *amqp.Channel
}

func (sC *GenericPublisher) SetChannel(channel *amqp.Channel, queueName string) {
	sC.channel = channel
	sC.queueName = queueName
}

func (gp *GenericPublisher) Publish(message []byte) error {
	// Implementação específica para publicar mensagem na fila
	if gp.queueName == "" {
		return errors.New("queueName não pode ser vazio")
	}

	err := gp.channel.Publish(
		gp.queueName,
		"",
		false,
		false,
		GetAmqPublishingOptions(message))

	if err != nil {
		return err
	}

	return nil
}
