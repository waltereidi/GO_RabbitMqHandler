package service

import "go_rabbitmqhandler/internal/interfaces"

type queueConfig struct {
	name            string
	abstractFactory interfaces.AbstractFactoryHandler
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
