package rabbitMq

import (
	"log"

	"github.com/streadway/amqp"
)

type RabbitMQConfig struct {
	Port      int
	Host      string
	QueueName []string
	Username  string
	Password  string
}
type Option func(*RabbitMQConfig)

func WithHost(host string) Option {
	return func(s *RabbitMQConfig) {
		s.Host = host
	}
}
func WithPort(port int) Option {
	return func(s *RabbitMQConfig) {
		s.Port = port
	}
}
func WithQueues(queueName []string) Option {
	return func(s *RabbitMQConfig) {
		s.QueueName = queueName
	}
}
func Username(username string) Option {
	return func(s *RabbitMQConfig) {
		s.Username = username
	}
}
func Password(password string) Option {
	return func(s *RabbitMQConfig) {
		s.Password = password
	}
}
func NewConnection(opts ...Option) *RabbitMQConfig {
	s := &RabbitMQConfig{
		Host: "localhost", // Default value
		Port: 5672,        // Default value
		QueueName: []string{"defaultqueue"}, // Default value
		Username: "admin", // Default value
		Password: "admin", // Default value],
	}

	for _, opt := range opts {
		opt(s)
	}
	return s
}
func (cho *RabbitMQConfig) ConfigureHost() (<-chan amqp.Delivery, error) {
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	cho.failOnError(err, "Erro ao conectar no RabbitMQ")
	defer conn.Close()

	// // 📡 Canal
	ch, err := conn.Channel()
	cho.failOnError(err, "Erro ao abrir canal")
	defer ch.Close()

	// // 📬 Declarar fila
	q, err := ch.QueueDeclare(
		"LLM_QUEUE", // nome
		true,        // durável
		false,       // auto-delete
		false,       // exclusiva
		false,       // no-wait
		nil,         // args
	)
	cho.failOnError(err, "Erro ao declarar fila")

	// 👂 Consumir mensagens
	msgs, err := ch.Consume(
		q.Name,
		"",    // consumer
		false, // auto-ack (false = manual)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	cho.failOnError(err, "Erro ao registrar consumer")
	return msgs, nil
}
func (FoE *RabbitMQConfig) failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
