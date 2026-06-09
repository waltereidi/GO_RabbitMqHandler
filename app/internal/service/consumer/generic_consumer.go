package consumer

import (
	"go_rabbitmqhandler/internal/interfaces"
	"go_rabbitmqhandler/internal/model"

	"github.com/streadway/amqp"
)

type GenericConsumer struct {
	config          model.ConsumerConfig
	delivery        <-chan amqp.Delivery
}

func (Cc *GenericConsumer) ConfigureConsumer(ch *amqp.Channel) error {
	q, err := ch.QueueDeclare(
		Cc.config.QueueName, // nome
		Cc.config.Durable,                // durável
		Cc.config.AutoDelete,               // auto-delete
		Cc.config.Exclusive,               // exclusiva
		Cc.config.NoWait,               // no-wait
		Cc.config.Args,                 // args
	)
	if err != nil {
		return err
	}
	// 👂 Consumir mensagens
	msgs, err := ch.Consume(
		q.Name,
		Cc.config.QueueName, // nome
		Cc.config.Durable,                // durável
		Cc.config.AutoDelete,               // auto-delete
		Cc.config.Exclusive,               // exclusiva
		Cc.config.NoWait,               // no-wait
		Cc.config.Args,                 // args
	)
	if err != nil {
		return err
	}
	Cc.delivery = msgs
}

func (c *GenericConsumer) Consume(ch *amqp.Channel){
	forever := make(chan bool)

	for d := range c.delivery {

		factory, err := c.config.AbstractFactory.CreateStrategy(&d.Body)
		if err != nil {
			hm.failOnError(err, "Erro ao obter factory")
		}

		strategy, err := factory.CreateStrategy(&d.Body)
		if err != nil {
			hm.failOnError(err, "Erro ao criar estratégia")
		}
		response, err := strategy.Start()

		if publisher != nil {
			err := publisher.Publish(response)
			if err != nil {
				hm.failOnError(err, "Erro ao publicar mensagem")
			}
		}
		// ⚙️ Processamento da mensagem
		//		err := hm.processMessage( factory, d.Body )

		if err != nil {
			//log.Printf("❌ Erro ao processar: %s", err)
			//d.Nack(false, true) // requeue
			d.Ack(false)
			continue
		}

		// ✅ Confirma processamento
		d.Ack(false)

	}
	<-forever

}
