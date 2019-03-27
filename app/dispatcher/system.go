package dispatcher

import "context"

type (
	//SystemService service for job CRUD.
	SystemService interface {
		HealthCheck(context.Context) (*HealthCheckReply, error)
	}

	//HealthCheckReply struct.
	HealthCheckReply struct{}
)
