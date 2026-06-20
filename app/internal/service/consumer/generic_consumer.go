package consumer

import (
	"go_rabbitmqhandler/internal/service/parser"
	"go_rabbitmqhandler/internal/service/publisher"

	"github.com/streadway/amqp"
)

type GenericConsumer struct {
	config          ConsumerConfig
	delivery        <-chan amqp.Delivery
	filterPublisher publisher.PublisherInterface
	logPublisher    publisher.PublisherInterface
}

func (sC *GenericConsumer) SetConfiguration(config *ConsumerConfig) {
	sC.config = *config
}
func (gC *GenericConsumer) configureConsumer(ch *amqp.Channel) error {
	q, err := ch.QueueDeclare(
		gC.config.QueueName,  // nome
		gC.config.Durable,    // durável
		gC.config.AutoDelete, // auto-delete
		gC.config.Exclusive,  // exclusiva
		gC.config.NoWait,     // no-wait
		gC.config.Args,       // args
	)
	println("declared queue ", gC.config.QueueName)
	if err != nil {
		return err
	}
	// 👂 Consumir mensagens
	msgs, err := ch.Consume(
		q.Name,
		gC.config.QueueName,  // nome
		gC.config.Durable,    // durável
		gC.config.AutoDelete, // auto-delete
		gC.config.Exclusive,  // exclusiva
		gC.config.NoWait,     // no-wait
		gC.config.Args,       // args
	)

	if err != nil {
		return err
	}
	gC.delivery = msgs
	gC.setFilterPublisher(ch)
	gC.setLogPublisher()

	return nil
}
func (cP *GenericConsumer) setLogPublisher() {
	cP.config.QueueName = "LogQueue"

}
func (cP *GenericConsumer) setFilterPublisher(ch *amqp.Channel) {
	publisher := publisher.GenericPublisher{}
	publisher.SetChannel(ch, "FilterQueue")

	cP.filterPublisher = &publisher
}
func (cP *GenericConsumer) getStrategy(message IntegrationEvent) (StrategyHandler, error) {
	strategy, err := cP.config.AbstractFactory.CreateStrategy(&message)

	if err != nil {
		return nil, err
	}

	return strategy, nil
}

func (c *GenericConsumer) Consume(ch *amqp.Channel) {
	c.configureConsumer(ch)
	println("end consumer configuration")
	forever := make(chan bool)

	for d := range c.delivery {
		parser := parser.JsonParser[IntegrationEvent]{}
		i := parser.NewParser()
		model, err := i.Decode(d.Body)
		if err != nil {
			c.publishErrorLog(err, ch, model)
			continue
		}

		strategy, err := c.getStrategy(model)
		if err != nil {
			c.publishErrorLog(err, ch, model)
			continue
		}

		response, err := strategy.Start()

		if c.filterPublisher != nil {
			model.ExchangePayload(response)
			err := c.filterPublisher.Publish(response)
			if err != nil {
				c.publishErrorLog(err, ch, model)
				continue
			}
		}

		if err != nil {
			c.publishErrorLog(err, ch, model)
			d.Ack(true)
			continue
		}

		// ✅ Confirma processamento
		d.Ack(true)

	}
	<-forever

}

func (gC *GenericConsumer) publishErrorLog(err error, ch *amqp.Channel, iE IntegrationEvent) {
	logPublisher := publisher.GenericPublisher{}
	logPublisher.SetChannel(ch, "LogQueue")
	iE.ExchangePayload([]byte(err.Error()))
	logPublisher.Publish([]byte(err.Error()))
}
