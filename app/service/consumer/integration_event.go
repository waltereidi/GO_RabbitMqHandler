package consumer

type RabbitMQHandlerConfig struct {
	Host            string
	Port            int
	Username        string
	Password        string
	Queue           string
	AbstractFactory FactoryHandler
}

type IntegrationEvent struct {
	EventName  string
	Payload    []byte
	OccuredAt  int64
	MetaHeader []MetaHeader
}
type MetaHeader struct {
	Source    string
	EventName string
	Args      []Args
	OccuredAt int64
}

type Args struct {
	Key   string
	Value string
}

func (iE *IntegrationEvent) GetNextQueue() (string, error) {
	return "", nil
}

func (iE *IntegrationEvent) CreateMetaHeader(
	source string,
	eventName string,
) MetaHeader {
	return MetaHeader{
		Source:    source,
		EventName: eventName,
	}
}

func (mH *MetaHeader) AddNextQueue(
	qn string,
) {
	mH.CreateArgs("QueueName", qn)
}

func (mH *MetaHeader) CreateArgs(
	key string,
	value string,
) Args {
	return Args{
		Key:   key,
		Value: value,
	}
}

func (eP *IntegrationEvent) ExchangePayload(
	payload []byte,
) {
	eP.Payload = payload
}
