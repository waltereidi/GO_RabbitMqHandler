package service

import (
	"go_rabbitmqhandler/internal/interfaces"

	"github.com/streadway/amqp"
)

type queueConfig struct {
	name            string
	abstractFactory interfaces.AbstractFactoryHandler
	publisher       *interfaces.Publisher
}

func ConfigureConsumer() Option {
	f := func(rmc *RabbitMQConfig) {
		for _, qn := range rmc.queueConfig {
			q, err := rmc.channel.QueueDeclare(
				qn.name, // nome
				true,    // durável
				false,   // auto-delete
				false,   // exclusiva
				false,   // no-wait
				nil,     // args
			)
			rmc.failOnError(err, "Erro ao declarar fila")

			// 👂 Consumir mensagens
			msgs, err := rmc.channel.Consume(
				q.Name,
				"",    // consumer
				false, // auto-ack (false = manual)
				false, // exclusive
				false, // no-local
				false, // no-wait
				nil,   // args
			)
			rmc.failOnError(err, "Erro ao registrar consumer")
			rmc.HandleMessage(msgs, qn.abstractFactory)

		}
		// // 📬 Declarar fila

	}
	return f
}

func (hm *RabbitMQConfig) HandleMessage(msgs <-chan amqp.Delivery,
	abstractFactory interfaces.AbstractFactoryHandler,
	publisher *interfaces.Publisher) {
	forever := make(chan bool)

	for d := range msgs {

		factory, err := abstractFactory.CreateStrategy(&d.Body)
		if err != nil {
			hm.failOnError(err, "Erro ao obter factory")
		}

		strategy, err := factory.CreateStrategy(&d.Body)
		if err != nil {
			hm.failOnError(err, "Erro ao criar estratégia")
		}
		response, err := strategy.Start()

		if publisher != nil {
			err := publisher.Publish("", response)
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
