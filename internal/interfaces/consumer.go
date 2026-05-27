package interfaces

import (
	"go_rabbitmqhandler/internal/models"
)

type Consumer interface {
	Consume(afh *AbstractFactoryHandler) error
	GetIdentity() models.ConsumerIdentity
}
