package consumer

import (
	"github.com/GoPersonalCluster/GO_RabbitMqHandler/app/service/parser"
	"github.com/GoPersonalCluster/GO_RabbitMqHandler/app/service/publisher"

	"github.com/streadway/amqp"
)

type FilterConsumer struct {
	config           ConsumerConfig
	delivery         <-chan amqp.Delivery
	genericPublisher publisher.PublisherInterface
	logPublisher     publisher.PublisherInterface
}

func (sC *FilterConsumer) SetConfiguration(config *ConsumerConfig) {
	sC.config = *config
}
func (cC *FilterConsumer) configureConsumer(ch *amqp.Channel) error {
	q, err := ch.QueueDeclare(
		cC.config.QueueName,  // nome
		cC.config.Durable,    // durável
		cC.config.AutoDelete, // auto-delete
		cC.config.Exclusive,  // exclusiva
		cC.config.NoWait,     // no-wait
		cC.config.Args,       // args
	)
	println("declared queue ", cC.config.QueueName)
	if err != nil {
		return err
	}
	// 👂 Consumir mensagens
	msgs, err := ch.Consume(
		q.Name,
		cC.config.QueueName,  // nome
		cC.config.Durable,    // durável
		cC.config.AutoDelete, // auto-delete
		cC.config.Exclusive,  // exclusiva
		cC.config.NoWait,     // no-wait
		cC.config.Args,       // args
	)

	if err != nil {
		return err
	}
	cC.delivery = msgs
	cC.setGenericPublisher(ch)
	cC.setLogPublisher()

	return nil
}
func (cP *FilterConsumer) setLogPublisher() {
	cP.config.QueueName = "LogQueue"

}
func (cP *FilterConsumer) setGenericPublisher(ch *amqp.Channel) {
	publisher := publisher.GenericPublisher{}
	cP.genericPublisher = &publisher
}
func (cP *FilterConsumer) getStrategy(message IntegrationEvent) (StrategyHandler, error) {
	strategy, err := cP.config.AbstractFactory.CreateStrategy(&message)

	if err != nil {
		return nil, err
	}

	return strategy, nil
}

func (c *FilterConsumer) Consume(ch *amqp.Channel) {
	c.configureConsumer(ch)

	println("end consumer configuration")
	forever := make(chan bool)

	for d := range c.delivery {

		parser := parser.JsonParser[IntegrationEvent]{}
		i := parser.NewParser()
		message, err := i.Decode(d.Body)
		if err != nil {
			c.publishErrorLog(err, ch, message)
			continue
		}
		strategy, err := c.getStrategy(message)
		if err != nil {
			c.publishErrorLog(err, ch, message)
			continue
		}

		response, err := strategy.Start()
		if err != nil {
			c.publishErrorLog(err, ch, message)
			d.Ack(true)
			continue
		}
		nq , err := message.GetNextQueue()
		if nq ==  "" {
			continue
		}

		if c.genericPublisher != nil {
			message.ExchangePayload(response)
			c.genericPublisher.SetChannel(ch , nq )
			err := c.genericPublisher.Publish(response)
			if err != nil {
				c.publishErrorLog(err, ch, message)
				continue
			}
		}

		if err != nil {
			c.publishErrorLog(err, ch, message)
			d.Ack(true)
			continue
		}

		d.Ack(true)

	}
	<-forever
}

func (gC *FilterConsumer) publishErrorLog(err error, ch *amqp.Channel, iE IntegrationEvent) {
	logPublisher := publisher.GenericPublisher{}
	logPublisher.SetChannel(ch, "LogQueue")
	iE.ExchangePayload([]byte(err.Error()))
	logPublisher.Publish([]byte(err.Error()))
}
