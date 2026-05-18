package interfaces

type AbstractFactoryHandler interface {
	CreateStrategy(body *[]byte) (FactoryHandler, error)
}
