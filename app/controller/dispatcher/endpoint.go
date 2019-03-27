//+build !test

package dispatcher

import (
	"context"

	"github.com/go-kit/kit/endpoint"
	"github.com/alokic/queue/app/controller"
)

// RegisterJobEndPoint allows transport layer to get register dispatcher for service.
func RegisterJobEndPoint(s controller.DispatcherService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.RegisterJob(ctx, request.(*controller.DispatcherJobRegisterRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// DeregisterJobEndPoint allows transport layer to get register dispatcher for service.
func DeregisterJobEndPoint(s controller.DispatcherService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.DeregisterJob(ctx, request.(*controller.DispatcherJobDeregisterRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// GossipEndPoint allows transport layer to gossip for service.
func GossipEndPoint(s controller.DispatcherService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.Gossip(ctx, request.(*controller.DispatcherGossipRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}
