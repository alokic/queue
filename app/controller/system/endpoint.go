package system

import (
	"context"

	"github.com/go-kit/kit/endpoint"
	"github.com/alokic/queue/app/controller"
)

// HealthCheckEndPoint allows transport layer for healthcheck.
func HealthCheckEndPoint(s controller.SystemService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.HealthCheck(ctx)
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}
