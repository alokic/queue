package system

import (
	"context"
	"time"

	"github.com/alokic/queue/app/controller"
	"github.com/honestbee/gopkg/logger"
)

type loggingService struct {
	logger  logger.Logger
	Service controller.SystemService
}

// NewLoggingService returns a new instance of a logging controller.SystemService.
func NewLoggingService(logger logger.Logger, s controller.SystemService) controller.SystemService {
	return &loggingService{
		logger:  logger,
		Service: s,
	}
}

// HealthCheck is logging middleware for service.
func (s *loggingService) HealthCheck(ctx context.Context) (out *controller.HealthCheckReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method HealthCheck took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.HealthCheck(ctx)
}
