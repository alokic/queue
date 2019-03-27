package system

import (
	"context"
	"github.com/alokic/queue/app/dispatcher"
	"github.com/honestbee/gopkg/logger"
)

type (
	systemService struct {
		logger logger.Logger
	}
)

//New constructor.
func New(logger logger.Logger) dispatcher.SystemService {
	lg := logger.ContextualLogger(map[string]interface{}{"context": "system-service"})
	return &systemService{logger: lg}
}

//TODO make it sane.
//HealthCheck handler.
func (ss *systemService) HealthCheck(ctx context.Context) (*dispatcher.HealthCheckReply, error) {
	rep := new(dispatcher.HealthCheckReply)
	return rep, nil
}
