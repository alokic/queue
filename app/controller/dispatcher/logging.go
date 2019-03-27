//+build !test

package dispatcher

import (
	"context"
	"time"

	"github.com/alokic/queue/app/controller"
	"github.com/honestbee/gopkg/logger"
)

type loggingService struct {
	logger  logger.Logger
	Service controller.DispatcherService
}

// NewLoggingService returns a new instance of a logging controller.DispatcherService.
func NewLoggingService(logger logger.Logger, s controller.DispatcherService) controller.DispatcherService {
	return &loggingService{
		logger:  logger,
		Service: s,
	}
}

// RegisterJob is logging middleware for service.
func (s *loggingService) RegisterJob(ctx context.Context, in *controller.DispatcherJobRegisterRequest) (out *controller.DispatcherJobRegisterReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method RegisterJob took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.RegisterJob(ctx, in)
}

// DeregisterJob is logging middleware for service.
func (s *loggingService) DeregisterJob(ctx context.Context, in *controller.DispatcherJobDeregisterRequest) (out *controller.DispatcherJobDeregisterReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method DeregisterJob took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.DeregisterJob(ctx, in)
}

// Gossip is logging middleware for service.
func (s *loggingService) Gossip(ctx context.Context, in *controller.DispatcherGossipRequest) (out *controller.DispatcherGossipReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method Gossip took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.Gossip(ctx, in)
}
