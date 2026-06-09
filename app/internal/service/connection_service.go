package service

import (
	"go_rabbitmqhandler/internal/interfaces"
	"log"

	"github.com/streadway/amqp"
)

type RabbitMQConfigComposite struct {
	channel    ChannelConfig
	connection *amqp.Connection
}
type ChannelConfig struct {
	publishers []interfaces.Publisher
	consumers  []interfaces.Consumer
	channel    *amqp.Channel
}

func FindOrElse[T any](
	items []T,
	predicate func(T) bool,
	orElse func() T,
) T {
	for _, item := range items {
		if predicate(item) {
			return item
		}
	}

	return orElse()
}

func (rmc *RabbitMQConfigComposite) AddConsumer(
	queueName string,
	abstractFactory interfaces.AbstractFactoryHandler,
	consumer interfaces.Consumer) {

	rmc.channel.consumers = append(rmc.channel.consumers, consumer)
}

func (rmc *RabbitMQConfigComposite) AddPublisher(publisher interfaces.Publisher) {
	rmc.channel.publishers = append(rmc.channel.publishers, publisher)
}

func (rmc *RabbitMQConfigComposite) ConfigureConnection() {
	evc := config.NewEnvironmentConfig()


	conn, err := amqp.Dial(fmt.Sprintf(`amqp://%s:%s@%s:%s/`, 
		evc.RabbitMQUsername, 
		evc.RabbitMQPassword, 
		evc.RabbitMQHost, 
		evc.RabbitMQPort))

	rmc.failOnError(err, "Erro ao conectar no RabbitMQ")
	
	defer conn.Close()

	// // 📡 Canal
	ch, err := conn.Channel()
	rmc.channel.channel = ch

	rmc.failOnError(err, "Erro ao abrir canal")
	defer rmc.CloseConnection()
}
func (rmc *RabbitMQConfigComposite) Start() error {
	err = rmc.isValidConfiguration() 
	if err != nil {
		return err
	}
	for _, consumer := range rmc.channel.consumers {
		go rmc.consumeAsync(consumer)		
	}
	

}

func (rmc *RabbitMQConfigComposite) consumeAsync(consumer interfaces.Consumer){
	
	consumer.Consume(rmc.channel.channel)


}


func (rmc *RabbitMQConfigComposite) isValidConfiguration() error {



}
func (rmc *RabbitMQConfigComposite) CloseConnection() {
	rmc.connection.Close()
}

func (FoE *RabbitMQConfigComposite) failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		//FoE.errors = append(FoE.errors, err)
	}
}
