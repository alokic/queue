//+build !test

package job

import (
	"context"
	"time"

	"github.com/alokic/queue/app/controller"
	"github.com/honestbee/gopkg/logger"
)

type loggingService struct {
	logger  logger.Logger
	Service controller.JobService
}

// NewLoggingService returns a new instance of a logging controller.JobService.
func NewLoggingService(logger logger.Logger, s controller.JobService) controller.JobService {
	return &loggingService{
		logger:  logger,
		Service: s,
	}
}

// RegisterJob is logging middleware for service.
func (s *loggingService) GetJob(ctx context.Context) (out *controller.GetJobReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method get took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.GetJob(ctx)
}

// CreateJob is logging middleware for service.
func (s *loggingService) CreateJob(ctx context.Context, in *controller.CreateJobRequest) (out *controller.CreateJobReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method create took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.CreateJob(ctx, in)
}

// UpdateJob is logging middleware for service.
func (s *loggingService) UpdateJob(ctx context.Context, in *controller.UpdateJobRequest) (out *controller.UpdateJobReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method update took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.UpdateJob(ctx, in)
}

// ArchiveJob is logging middleware for service.
func (s *loggingService) ArchiveJob(ctx context.Context, in *controller.ArchiveJobRequest) (out *controller.ArchiveJobReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method delete took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.ArchiveJob(ctx, in)
}

// StartJob is logging middleware for service.
func (s *loggingService) StartJob(ctx context.Context, in *controller.StartJobRequest) (out *controller.StartJobReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method startjob took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.StartJob(ctx, in)
}

// StopJob is logging middleware for service.
func (s *loggingService) StopJob(ctx context.Context, in *controller.StopJobRequest) (out *controller.StopJobReply, err error) {
	defer func(begin time.Time) {
		s.logger.Infof("method stopjob took: %v, err: %v", time.Since(begin), err)
	}(time.Now())
	return s.Service.StopJob(ctx, in)
}
