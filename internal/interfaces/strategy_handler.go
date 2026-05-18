package interfaces

type StrategyHandler interface {
	Start() ([]byte, error)
}
