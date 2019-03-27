package worker

import (
	"context"
	"time"

	"github.com/alokic/queue/app/dispatcher"
	"github.com/honestbee/gopkg/logger"
)

type loggingService struct {
	logger  logger.Logger
	Service dispatcher.WorkerService
}

// NewLoggingService returns a new instance of a logging authorizer.UserService.
func NewLoggingService(s dispatcher.WorkerService, lg logger.Logger) dispatcher.WorkerService {
	logger := lg.ContextualLogger(map[string]interface{}{"context": "logging-service"})
	return &loggingService{
		logger:  logger,
		Service: s,
	}
}

func (s *loggingService) Init() {}

// Subscribe is logging middleware for service.
func (s *loggingService) SubscribeJob(ctx context.Context, in *dispatcher.WorkerSubscribeJobRequest) (out *dispatcher.WorkerSubscribeJobReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method SubscribeJob took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.SubscribeJob(ctx, in)
}

// UnsubscribeJob is logging middleware for service.
func (s *loggingService) UnsubscribeJob(ctx context.Context, in *dispatcher.WorkerUnsubscribeJobRequest) (out *dispatcher.WorkerUnsubscribeJobReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method UnsubscribeJob took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.UnsubscribeJob(ctx, in)
}

// GetMsgs is logging middleware for service.
func (s *loggingService) GetMsgs(ctx context.Context, in *dispatcher.WorkerGetMsgsRequest) (out *dispatcher.WorkerGetMsgsReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method GetMsgs took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.GetMsgs(ctx, in)
}

// GetRetryMsgs is logging middleware for service.
func (s *loggingService) GetRetryMsgs(ctx context.Context, in *dispatcher.WorkerGetRetryMsgsRequest) (out *dispatcher.WorkerGetRetryMsgsReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method GetRetryMsgs took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.GetRetryMsgs(ctx, in)
}

// GetDLQMsgs is logging middleware for service.
func (s *loggingService) GetDLQMsgs(ctx context.Context, in *dispatcher.WorkerGetDLQMsgsRequest) (out *dispatcher.WorkerGetDLQMsgsReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method GetDLQMsgs took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.GetDLQMsgs(ctx, in)
}

// AckMsgs is logging middleware for service.
func (s *loggingService) AckMsgs(ctx context.Context, in *dispatcher.WorkerAckMsgsRequest) (out *dispatcher.WorkerAckMsgsReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method AckMsgs took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.AckMsgs(ctx, in)
}

// NackMsgs is logging middleware for service.
func (s *loggingService) NackMsgs(ctx context.Context, in *dispatcher.WorkerNackMsgsRequest) (out *dispatcher.WorkerNackMsgsReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method NackMsgs took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.NackMsgs(ctx, in)
}

// PublishMsg is logging middleware for service.
func (s *loggingService) PublishMsg(ctx context.Context, in *dispatcher.WorkerPublishMsgsRequest) (out *dispatcher.WorkerPublishMsgsReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method PublishMsg took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.PublishMsg(ctx, in)
}

// Stop is logging middleware for service.
func (s *loggingService) Stop() {
	s.Service.Stop()
}
