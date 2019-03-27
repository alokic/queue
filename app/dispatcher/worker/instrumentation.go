//+build !test

package worker

import (
	"context"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/alokic/queue/app/dispatcher"
)

type instrumentingService struct {
	requestCount   metrics.Counter
	requestLatency metrics.Histogram
	Service        dispatcher.WorkerService
}

// NewInstrumentingService returns an instance of an instrumenting authorizer.UserService.
func NewInstrumentingService(counter metrics.Counter, latency metrics.Histogram, s dispatcher.WorkerService) dispatcher.WorkerService {
	return &instrumentingService{
		requestCount:   counter,
		requestLatency: latency,
		Service:        s,
	}
}

func (s *instrumentingService) Init() {

}

// Subscribe is instrumentation middleware for subscribe usecase of service.
func (s *instrumentingService) SubscribeJob(ctx context.Context, in *dispatcher.WorkerSubscribeJobRequest) (out *dispatcher.WorkerSubscribeJobReply, err error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "Subscribe").Add(1)
		s.requestLatency.With("method", "Subscribe").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.SubscribeJob(ctx, in)
}

// Unsubscribe is instrumentation middleware for unsubscribe usecase of service.
func (s *instrumentingService) UnsubscribeJob(ctx context.Context, in *dispatcher.WorkerUnsubscribeJobRequest) (out *dispatcher.WorkerUnsubscribeJobReply, err error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "Unsubscribe").Add(1)
		s.requestLatency.With("method", "Unsubscribe").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.UnsubscribeJob(ctx, in)
}

// GetMsgs is instrumentation middleware for GetMsgs usecase of service.
func (s *instrumentingService) GetMsgs(ctx context.Context, in *dispatcher.WorkerGetMsgsRequest) (out *dispatcher.WorkerGetMsgsReply, err error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "GetMsgs").Add(1)
		s.requestLatency.With("method", "GetMsgs").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.GetMsgs(ctx, in)
}

// GetRetryMsgs is instrumentation middleware for GetRetryMsgs usecase of service.
func (s *instrumentingService) GetRetryMsgs(ctx context.Context, in *dispatcher.WorkerGetRetryMsgsRequest) (out *dispatcher.WorkerGetRetryMsgsReply, err error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "GetRetryMsgs").Add(1)
		s.requestLatency.With("method", "GetRetryMsgs").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.GetRetryMsgs(ctx, in)
}

// GetDLQMsgs is instrumentation middleware for GetDLQMsgs usecase of service.
func (s *instrumentingService) GetDLQMsgs(ctx context.Context, in *dispatcher.WorkerGetDLQMsgsRequest) (out *dispatcher.WorkerGetDLQMsgsReply, err error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "GetDLQMsgs").Add(1)
		s.requestLatency.With("method", "GetDLQMsgs").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.GetDLQMsgs(ctx, in)
}

// AckMsgs is instrumentation middleware for AckMsgs usecase of service.
func (s *instrumentingService) AckMsgs(ctx context.Context, in *dispatcher.WorkerAckMsgsRequest) (out *dispatcher.WorkerAckMsgsReply, err error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "ack").Add(1)
		s.requestLatency.With("method", "ack").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.AckMsgs(ctx, in)
}

// NackMsgs is instrumentation middleware for NackMsgs usecase of service.
func (s *instrumentingService) NackMsgs(ctx context.Context, in *dispatcher.WorkerNackMsgsRequest) (out *dispatcher.WorkerNackMsgsReply, err error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "nack").Add(1)
		s.requestLatency.With("method", "nack").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.NackMsgs(ctx, in)
}

// PublishMsg is instrumentation middleware for PublishMsg usecase of service.
func (s *instrumentingService) PublishMsg(ctx context.Context, in *dispatcher.WorkerPublishMsgsRequest) (out *dispatcher.WorkerPublishMsgsReply, err error) {
	defer func(begin time.Time) {
		s.requestCount.With("method", "PublishMsg").Add(1)
		s.requestLatency.With("method", "PublishMsg").Observe(time.Since(begin).Seconds())
	}(time.Now())

	return s.Service.PublishMsg(ctx, in)
}

// Stop is instrumentation middleware for Stop usecase of service.
func (s *instrumentingService) Stop() {
	s.Service.Stop()
}
