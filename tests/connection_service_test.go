package math

import (
	"fmt"
	"go_rabbitmqhandler/internal/service"
	"testing"
)

func TestWithHost(t *testing.T) {
	svc := service.RabbitMQConfig{}
	result, errs := svc.Configure(service.WithHost("testhost"))

	if len(errs) > 0 || result != nil {
		fmt.Printf("%v\n", errs)
	}
}
