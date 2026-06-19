package service_test

import (
	"bytes"
	"go_rabbitmqhandler/internal/service"
	"go_rabbitmqhandler/internal/service/consumer"
	"testing"
)

func TestConnectionService(t *testing.T) {
	service := service.RabbitMQConfigComposite{}
	service.ConfigureConnection()
	factory := TestConcreteFactory{}

	event := &consumer.IntegrationEvent{
		EventName: "TestEvent",
	}
	consumer.GenericConsumer()

	service.AddConsumer("test_queue",nil)

}

func TestIntegrationEvent_ExchangePayload(t *testing.T) {
	event := consumer.IntegrationEvent{}

	expectedPayload := []byte("new payload")

	event.ExchangePayload(expectedPayload)

	if !bytes.Equal(event.Payload, expectedPayload) {
		t.Errorf(
			"payload incorreto. esperado=%v obtido=%v",
			expectedPayload,
			event.Payload,
		)
	}
}

func TestIntegrationEvent_Create(t *testing.T) {
	event := consumer.IntegrationEvent{
		EventName: "UserCreated",
		Payload:   []byte(`{"id":1}`),
		OccuredAt: 123456789,
	}

	if event.EventName != "UserCreated" {
		t.Errorf("EventName esperado UserCreated, obtido %s", event.EventName)
	}

	if event.OccuredAt != 123456789 {
		t.Errorf("OccuredAt esperado 123456789, obtido %d", event.OccuredAt)
	}
}

type TestConcreteFactory struct {
}

func (cS *TestConcreteFactory) CreateStrategy(
	event *consumer.IntegrationEvent,
) (consumer.StrategyHandler, error) {
	return &TestConcreteStrategy{}, nil
}

type TestConcreteStrategy struct {
}

func (s *TestConcreteStrategy) Start() ([]byte, error) {
	return []byte("success"), nil
}

// func TestConcreteFactory_CreateStrategy(t *testing.T) {
// 	factory := TestConcreteFactory{}

// 	event := &consumer.IntegrationEvent{
// 		EventName: "TestEvent",
// 	}

// 	strategy, err := factory.CreateStrategy(event)

// 	if err != nil {
// 		t.Fatalf("erro inesperado: %v", err)
// 	}

// 	if strategy == nil {
// 		t.Fatal("strategy não deveria ser nil")
// 	}
// }

func TestConcreteStrategy_Start(t *testing.T) {
	strategy := TestConcreteStrategy{}

	result, err := strategy.Start()

	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}

	expected := []byte("success")

	if !bytes.Equal(result, expected) {
		t.Errorf(
			"resultado incorreto. esperado=%v obtido=%v",
			expected,
			result,
		)
	}
}
