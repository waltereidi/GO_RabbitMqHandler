package consumer

type FactoryHandler interface {
	CreateStrategy(event *IntegrationEvent) (StrategyHandler, error)
}
type FilterFactory interface {
	GetQueue(event *IntegrationEvent) (IntegrationEvent, error)
}
