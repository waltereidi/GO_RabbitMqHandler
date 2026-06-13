package model

type IntegrationEvent struct {
	EventName  string
	Payload    []byte
	OccuredAt  int64
	MetaHeader []struct {
		Source    string
		EventName string
		OccuredAt int64
	}
}
