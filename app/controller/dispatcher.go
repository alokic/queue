package controller

import (
	"context"
	"github.com/go-playground/validator"
	"github.com/alokic/queue/pkg"
)

type (
	//DispatcherJob struct.
	DispatcherJob struct {
		ID           uint64 `json:"id"`
		DispatcherID uint64 `json:"dispatcher_id"`
		JobID        uint64 `json:"job_id"`
	}

	//DispatcherJobRegisterRequest request.
	DispatcherJobRegisterRequest struct {
		DispatcherID uint64 `json:"dispatcher_id" validate:"required"`
		JobName      string `json:"job_name" validate:"required"`
	}

	//DispatcherJobRegisterReply request.
	DispatcherJobRegisterReply struct {
		Job *pkg.Job `json:"job"`
	}

	//DispatcherJobDeregisterRequest request.
	DispatcherJobDeregisterRequest struct {
		DispatcherID uint64 `json:"dispatcher_id" validate:"required"`
		JobID        uint64 `json:"job_id" validate:"required"`
	}

	//DispatcherJobDeregisterReply request.
	DispatcherJobDeregisterReply struct{}

	//DispatcherGossipRequest request.
	DispatcherGossipRequest struct {
		DispatcherID uint64 `json:"dispatcher_id" validate:"required"`
	}

	//DispatcherGossipReply request.
	DispatcherGossipReply struct {
		Jobs []*pkg.Job `json:"jobs"`
	}

	//DispatcherService service for dispatcher interaction.
	DispatcherService interface {
		RegisterJob(context.Context, *DispatcherJobRegisterRequest) (*DispatcherJobRegisterReply, error)
		DeregisterJob(context.Context, *DispatcherJobDeregisterRequest) (*DispatcherJobDeregisterReply, error)
		Gossip(context.Context, *DispatcherGossipRequest) (*DispatcherGossipReply, error)
	}
)

//Validate validates struct.
func (d *DispatcherJobRegisterRequest) Validate() error {
	return validator.New().Struct(d)
}

//Validate validates struct.
func (d *DispatcherJobDeregisterRequest) Validate() error {
	return validator.New().Struct(d)
}

//Validate validates struct.
func (d *DispatcherGossipRequest) Validate() error {
	return validator.New().Struct(d)
}
