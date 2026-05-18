package interfaces

type FactoryHandler interface {
	CreateStrategy(body *[]byte) (StrategyHandler, error)
}
