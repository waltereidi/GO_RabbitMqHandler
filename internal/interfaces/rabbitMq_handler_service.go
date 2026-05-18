package interfaces

type RabbitMQHandlerService interface {
	CreateConnection() error
	DeclareQueue(queueName string) error
	ConsumeMessages(queueName string) (<-chan []byte, error)
	AckMessage(deliveryTag uint64) error
}
