package publisher

import (
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

type GenericPublisher struct {
	queueName string
}

func (sqn *GenericPublisher) SetChannel(channel *amqp.Channel) {
	// Implementação específica para configurar o canal do publisher
	// Exemplo: sqn.channel = channel
}

func (gp *GenericPublisher) Publish(message []byte, channel *amqp.Channel) error {
	// Implementação específica para publicar mensagem na fila
	if gp.queueName == "" {
		return errors.New("queueName não pode ser vazio")
	}

	err := channel.Publish(
		gp.queueName,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})

	if err != nil {
		return err
	}

	return nil
}
