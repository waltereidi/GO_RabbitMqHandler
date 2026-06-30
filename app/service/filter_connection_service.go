package service

import (
	"fmt"
	"log"

	"github.com/GoPersonalCluster/GO_RabbitMqHandler/app/config"
	"github.com/GoPersonalCluster/GO_RabbitMqHandler/app/service/consumer"
	"github.com/GoPersonalCluster/GO_RabbitMqHandler/app/service/publisher"

	"github.com/streadway/amqp"
)

type FilterRabbitMQConfigComposite struct {
	channel    ChannelConfig
	connection *amqp.Connection
}
type FilterChannelConfig struct {
	publishers []publisher.PublisherInterface
	consumers  []consumer.Consumer
	channel    *amqp.Channel
}

func (rmc *FilterRabbitMQConfigComposite) AddConsumer(
	queueName string,
	consumer consumer.Consumer) {

	rmc.channel.consumers = append(rmc.channel.consumers, consumer)
}

func (rmc *FilterRabbitMQConfigComposite) ConfigureConnection() {
	evc := config.NewEnvironmentConfig()

	conn, err := amqp.Dial(fmt.Sprintf(`amqp://%s:%s@%s:%s/`,
		evc.RabbitMQUsername,
		evc.RabbitMQPassword,
		evc.RabbitMQHost,
		evc.RabbitMQPort))

	rmc.failOnError(err, "Erro ao conectar no RabbitMQ")

	//defer conn.Close()

	// // 📡 Canal
	ch, err := conn.Channel()
	rmc.channel.channel = ch

	rmc.failOnError(err, "Erro ao abrir canal")
	//defer rmc.CloseConnection()
}

func (rmc *FilterRabbitMQConfigComposite) Start() error {
	err := rmc.isValidConfiguration()
	if err != nil {
		return err
	}

	for _, consumer := range rmc.channel.consumers {
		go rmc.consumeAsync(consumer)
	}
	return nil
}
func (rmc *FilterRabbitMQConfigComposite) TestPublish() {
	var publisher = publisher.GenericPublisher{}
	publisher.SetChannel(rmc.channel.channel, "test_queue")
	publisher.Publish([]byte("test"))
}
func (rmc *FilterRabbitMQConfigComposite) consumeAsync(consumer consumer.Consumer) {
	print("start")

	consumer.Consume(rmc.channel.channel)
}

func (rmc *FilterRabbitMQConfigComposite) isValidConfiguration() error {
	return nil
}

func (FoE *FilterRabbitMQConfigComposite) failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		//FoE.errors = append(FoE.errors, err)
	}
}
