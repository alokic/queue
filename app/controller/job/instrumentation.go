//+build !test

package job

import (
	"context"
	"github.com/go-kit/kit/metrics"
	"github.com/alokic/queue/app/controller"
	"time"
)

type instrumentingService struct {
	requestCount   metrics.Counter
	requestLatency metrics.Histogram
	Service        controller.JobService
}

// NewInstrumentingService returns an instance of an instrumenting authorizer.UserService.
func NewInstrumentingService(counter metrics.Counter, latency metrics.Histogram, s controller.JobService) controller.JobService {
	return &instrumentingService{
		requestCount:   counter,
		requestLatency: latency,
		Service:        s,
	}
}

// RegisterJob is instrumentation middleware for create usecase of service.
func (s *instrumentingService) GetJob(ctx context.Context) (*controller.GetJobReply, error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "get").Add(1)
		s.requestLatency.With("method", "get").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.GetJob(ctx)
}

// CreateJob is instrumentation middleware for create usecase of service.
func (s *instrumentingService) CreateJob(ctx context.Context, in *controller.CreateJobRequest) (*controller.CreateJobReply, error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "create").Add(1)
		s.requestLatency.With("method", "create").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.CreateJob(ctx, in)
}

// UpdateJob is instrumentation middleware for update usecase of service.
func (s *instrumentingService) UpdateJob(ctx context.Context, in *controller.UpdateJobRequest) (*controller.UpdateJobReply, error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "update").Add(1)
		s.requestLatency.With("method", "update").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.UpdateJob(ctx, in)
}

// ArchiveJob is instrumentation middleware for delete usecase of service.
func (s *instrumentingService) ArchiveJob(ctx context.Context, in *controller.ArchiveJobRequest) (*controller.ArchiveJobReply, error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "delete").Add(1)
		s.requestLatency.With("method", "delete").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.ArchiveJob(ctx, in)
}

// StartJob is instrumentation middleware for ratelimit usecase of service.
func (s *instrumentingService) StartJob(ctx context.Context, in *controller.StartJobRequest) (*controller.StartJobReply, error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "startjob").Add(1)
		s.requestLatency.With("method", "startjob").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.StartJob(ctx, in)
}

// StopJob is instrumentation middleware for ratelimit usecase of service.
func (s *instrumentingService) StopJob(ctx context.Context, in *controller.StopJobRequest) (*controller.StopJobReply, error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "stopjob").Add(1)
		s.requestLatency.With("method", "stopjob").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.StopJob(ctx, in)
}
