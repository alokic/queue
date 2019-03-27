//+build !test

package dispatcher

import (
	"context"
	"github.com/go-kit/kit/metrics"
	"github.com/alokic/queue/app/controller"
	"time"
)

type instrumentingService struct {
	requestCount   metrics.Counter
	requestLatency metrics.Histogram
	Service        controller.DispatcherService
}

// NewInstrumentingService returns an instance of an instrumenting controller.JobConfService.
func NewInstrumentingService(counter metrics.Counter, latency metrics.Histogram, s controller.DispatcherService) controller.DispatcherService {
	return &instrumentingService{
		requestCount:   counter,
		requestLatency: latency,
		Service:        s,
	}
}

// RegisterJob is instrumentation middleware for RegisterJob usecase of service.
func (s *instrumentingService) RegisterJob(ctx context.Context, in *controller.DispatcherJobRegisterRequest) (*controller.DispatcherJobRegisterReply, error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "RegisterJob").Add(1)
		s.requestLatency.With("method", "RegisterJob").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.RegisterJob(ctx, in)
}

// DeregisterJob is instrumentation middleware for RegisterJob usecase of service.
func (s *instrumentingService) DeregisterJob(ctx context.Context, in *controller.DispatcherJobDeregisterRequest) (*controller.DispatcherJobDeregisterReply, error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "DeregisterJob").Add(1)
		s.requestLatency.With("method", "DeregisterJob").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.DeregisterJob(ctx, in)
}

// Gossip is instrumentation middleware for Gossip usecase of service.
func (s *instrumentingService) Gossip(ctx context.Context, in *controller.DispatcherGossipRequest) (*controller.DispatcherGossipReply, error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "Gossip").Add(1)
		s.requestLatency.With("method", "Gossip").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.Gossip(ctx, in)
}
