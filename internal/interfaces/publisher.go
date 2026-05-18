package interfaces

type Publisher interface {
	Publish(queueName string, message []byte) error
}
