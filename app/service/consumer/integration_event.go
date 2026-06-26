package consumer
import "errors"
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

func (gQN *MetaHeader) GetQueueName()(string, error) {
	if(len( gQN.Args) < 1){
		return "",errors.New("Args is not set for this meta header " + gQN.EventName)
	}
	for i := 0; i < len( gQN.Args); i++ {
		if(gQN.Args[i].Key == "Queue" )
	}
}

type Args struct {
	Key   string
	Value string
}

func (gNQ *IntegrationEvent) GetNextQueue(string, error) {

}

func (eP *IntegrationEvent) AddMetaHeader(
	source string,
	eventName string,
	args []struct {
		Key   string
		Value string
	},

) {

}
func (eP *IntegrationEvent) ExchangePayload(
	payload []byte,
) {
	eP.Payload = payload
}
