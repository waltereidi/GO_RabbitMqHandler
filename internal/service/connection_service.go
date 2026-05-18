package service

import (
	"go_rabbitmqhandler/internal/interfaces"
	"log"

	"github.com/streadway/amqp"
)

type RabbitMQConfig struct {
	port        int
	host        string
	queueConfig []queueConfig
	username    string
	password    string
	errors      []error
	channel     *amqp.Channel
}

// type queueConfig struct {
// 	name            string
// 	abstractFactory interfaces.AbstractFactoryHandler
// }

type Option func(*RabbitMQConfig)

func WithHost(host string) Option {
	return func(s *RabbitMQConfig) {
		s.host = host
	}
}
func WithPort(port int) Option {
	return func(s *RabbitMQConfig) {
		s.port = port
	}
}
func AddQueue(queueName string, abstractFactory interfaces.AbstractFactoryHandler) Option {
	return func(s *RabbitMQConfig) {
		s.queueConfig = append(s.queueConfig,
			queueConfig{
				name:            queueName,
				abstractFactory: abstractFactory,
			},
		)
	}
}
func Username(username string) Option {
	return func(s *RabbitMQConfig) {
		s.username = username
	}
}
func Password(password string) Option {
	return func(s *RabbitMQConfig) {
		s.password = password
	}
}
func NewConnection(opts ...Option) *RabbitMQConfig {
	s := &RabbitMQConfig{
		host:        "localhost",                           // Default value
		port:        5672,                                  // Default value
		queueConfig: []queueConfig{{name: "defaultqueue"}}, // Default value
		username:    "admin",                               // Default value
		password:    "admin",                               // Default value],
	}

	for _, opt := range opts {
		opt(s)
	}
	return s
}

func ConfigureConnection() Option {
	return func(rmc *RabbitMQConfig) {

		conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
		rmc.failOnError(err, "Erro ao conectar no RabbitMQ")
		defer conn.Close()

		// // 📡 Canal
		ch, err := conn.Channel()
		rmc.failOnError(err, "Erro ao abrir canal")
		defer ch.Close()
		rmc.channel = ch
	}
}

// func ConfigureQueue() Option {
// 	f := func(rmc *RabbitMQConfig) {
// 		for _, qn := range rmc.queueConfig {
// 			q, err := rmc.channel.QueueDeclare(
// 				qn.name, // nome
// 				true,    // durável
// 				false,   // auto-delete
// 				false,   // exclusiva
// 				false,   // no-wait
// 				nil,     // args
// 			)
// 			rmc.failOnError(err, "Erro ao declarar fila")

// 			// 👂 Consumir mensagens
// 			msgs, err := rmc.channel.Consume(
// 				q.Name,
// 				"",    // consumer
// 				false, // auto-ack (false = manual)
// 				false, // exclusive
// 				false, // no-local
// 				false, // no-wait
// 				nil,   // args
// 			)
// 			rmc.failOnError(err, "Erro ao registrar consumer")
// 			rmc.HandleMessage(msgs, qn.abstractFactory)

// 		}
// 		// // 📬 Declarar fila

// 	}
// 	return f
// }

func (hm *RabbitMQConfig) HandleMessage(msgs <-chan amqp.Delivery, abstractFactory interfaces.AbstractFactoryHandler) {
	forever := make(chan bool)

	for d := range msgs {
		log.Printf("📥 Mensagem recebida: %s", d.Body)

		factory, err := abstractFactory.CreateStrategy(&d.Body)
		if err != nil {
			hm.failOnError(err, "Erro ao obter factory")
		}

		strategy, err := factory.CreateStrategy(&d.Body)
		if err != nil {
			hm.failOnError(err, "Erro ao criar estratégia")
		}
		strategy.Start()

		// ⚙️ Processamento da mensagem
		err := hm.processMessage(factory, d.Body)
		if err != nil {
			log.Printf("❌ Erro ao processar: %s", err)
			//d.Nack(false, true) // requeue
			d.Ack(false)
			continue
		}

		// ✅ Confirma processamento
		d.Ack(false)

	}
	<-forever

}

// func (cho *RabbitMQConfig) configureHost() {
// 	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
// 	cho.failOnError(err, "Erro ao conectar no RabbitMQ")
// 	defer conn.Close()

// 	// // 📡 Canal
// 	ch, err := conn.Channel()
// 	cho.failOnError(err, "Erro ao abrir canal")
// 	defer ch.Close()

// 	// // 📬 Declarar fila
// 	q, err := ch.QueueDeclare(
// 		"LLM_QUEUE", // nome
// 		true,        // durável
// 		false,       // auto-delete
// 		false,       // exclusiva
// 		false,       // no-wait
// 		nil,         // args
// 	)
// 	cho.failOnError(err, "Erro ao declarar fila")

//		// 👂 Consumir mensagens
//		msgs, err := ch.Consume(
//			q.Name,
//			"",    // consumer
//			false, // auto-ack (false = manual)
//			false, // exclusive
//			false, // no-local
//			false, // no-wait
//			nil,   // args
//		)
//		cho.failOnError(err, "Erro ao registrar consumer")
//		return msgs, nil
//	}
func (FoE *RabbitMQConfig) failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
