//+build !test

package job

import (
	"context"

	"github.com/go-kit/kit/endpoint"
	"github.com/alokic/queue/app/controller"
)

// GetJobEndPoint allows transport layer to get consumer job for service.
func GetJobEndPoint(s controller.JobService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.GetJob(ctx)
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// CreateJobEndPoint allows transport layer to create jobconfig for service.
func CreateJobEndPoint(s controller.JobService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.CreateJob(ctx, request.(*controller.CreateJobRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// UpdateJobEndPoint allows transport layer to update jobconfig for service.
func UpdateJobEndPoint(s controller.JobService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.UpdateJob(ctx, request.(*controller.UpdateJobRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// ArchiveJobEndPoint allows transport layer to delete jobconfig for service.
func ArchiveJobEndPoint(s controller.JobService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.ArchiveJob(ctx, request.(*controller.ArchiveJobRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// StartJobEndPoint allows transport layer to ratelimit jobs of a jobconfig for service.
func StartJobEndPoint(s controller.JobService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.StartJob(ctx, request.(*controller.StartJobRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// StopJobEndPoint allows transport layer to ratelimit jobs of a jobconfig for service.
func StopJobEndPoint(s controller.JobService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.StopJob(ctx, request.(*controller.StopJobRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}
