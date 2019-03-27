package system

import (
	"context"
	"time"

	"github.com/alokic/queue/app/dispatcher"
	"github.com/honestbee/gopkg/logger"
)

type loggingService struct {
	logger  logger.Logger
	Service dispatcher.SystemService
}

// NewLoggingService returns a new instance of a logging controller.SystemService.
func NewLoggingService(logger logger.Logger, s dispatcher.SystemService) dispatcher.SystemService {
	return &loggingService{
		logger:  logger,
		Service: s,
	}
}

// HealthCheck is logging middleware for service.
func (s *loggingService) HealthCheck(ctx context.Context) (out *dispatcher.HealthCheckReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method HealthCheck took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.HealthCheck(ctx)
}
