package controller

import (
	"context"
	"fmt"
	"github.com/go-playground/validator"
	"github.com/alokic/queue/pkg"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

type (
	//JobService service for job CRUD.
	JobService interface {
		GetJob(context.Context) (*GetJobReply, error)
		CreateJob(context.Context, *CreateJobRequest) (*CreateJobReply, error)
		UpdateJob(context.Context, *UpdateJobRequest) (*UpdateJobReply, error)
		ArchiveJob(context.Context, *ArchiveJobRequest) (*ArchiveJobReply, error)
		StartJob(context.Context, *StartJobRequest) (*StartJobReply, error)
		StopJob(context.Context, *StopJobRequest) (*StopJobReply, error)
	}

	//GetJobReply struct.
	GetJobReply struct {
		Job pkg.Job `json:"job"`
	}

	//AllJobReply struct.
	AllJobReply struct {
		Jobs []pkg.Job `json:"jobs"`
	}

	//CreateJobRequest struct.
	CreateJobRequest struct {
		Name              string           `json:"name" validate:"required,max=100"`
		Description       string           `json:"description" validate:"required,max=100"`
		Type              pkg.JobType      `json:"type" validate:"required,oneof=consumer producer"`
		QueueConfig       pkg.QueueConfig  `json:"queue_config" validate:"required"`
		RetryConfig       *pkg.RetryConfig `json:"retry_config"`
		MaxProcessingTime int              `json:"max_processing_time_ms" validate:"min=1"`
	}
	//CreateJobReply struct.
	CreateJobReply struct{}

	//UpdateJobRequest struct.
	UpdateJobRequest struct {
		Description       string           `json:"description" validate:"omitempty,max=100"`
		QueueConfig       *pkg.QueueConfig `json:"queue_config" validate:"omitempty"`
		RetryConfig       *pkg.RetryConfig `json:"retry_config"`
		MaxProcessingTime int              `json:"max_processing_time_ms" validate:"omitempty,min=1"`
	}
	//UpdateJobReply struct.
	UpdateJobReply struct{}

	//ArchiveJobRequest struct.
	ArchiveJobRequest struct{}
	//ArchiveJobReply struct.
	ArchiveJobReply struct{}

	//StartJobRequest struct.
	StartJobRequest struct{}
	//StartJobReply struct.
	StartJobReply struct{}

	//StopJobRequest struct.
	StopJobRequest struct{}
	//StopJobReply struct.
	StopJobReply struct{}
)

//Validate CreateJobRequest.
func (jc *CreateJobRequest) Validate() error {
	if err := validator.New().Struct(jc); err != nil {
		return err
	}

	switch jc.QueueConfig.Type {
	case pkg.KafkaQueue:
		qc := new(pkg.KafkaConfig)
		//TODO check if below works for nil too
		if err := mapstructure.Decode(jc.QueueConfig.Config, qc); err != nil {
			return errors.New(fmt.Sprintf("bad kafka config: %v", jc.QueueConfig))
		}
		if err := validator.New().Struct(qc); err != nil {
			return errors.Wrap(err, "bad kafka queue config")
		}
		jc.QueueConfig.Config = qc
	default:
		return errors.New(fmt.Sprintf("unsupported queue_type: %v", jc.QueueConfig.Type))
	}

	if jc.Type == pkg.ConsumerJob {
		if jc.RetryConfig == nil {
			return errors.New("please set retry_config")
		}

		if err := validator.New().Struct(jc.RetryConfig); err != nil {
			return errors.Wrap(err, "bad retry_config")
		}
	}

	return nil
}

//Validate UpdateJobRequest.
func (jc *UpdateJobRequest) Validate() error {
	if err := validator.New().Struct(jc); err != nil {
		return err
	}

	if jc.QueueConfig != nil {
		switch jc.QueueConfig.Type {
		case pkg.KafkaQueue:
			qc := new(pkg.KafkaConfig)
			//TODO check if below works for nil too
			if err := mapstructure.Decode(jc.QueueConfig.Config, qc); err != nil {
				return errors.New(fmt.Sprintf("bad kafka config: %v", jc.QueueConfig))
			}
			if err := validator.New().Struct(qc); err != nil {
				return errors.Wrap(err, "bad kafka queue config")
			}
			jc.QueueConfig.Config = qc
		default:
			return errors.New(fmt.Sprintf("unsupported queue_type: %v", jc.QueueConfig.Type))
		}
	}

	if jc.RetryConfig != nil {
		if err := validator.New().Struct(jc.RetryConfig); err != nil {
			return errors.Wrap(err, "bad retry_config")
		}
	}

	return nil
}

//Validate ArchiveJobRequest.
func (jc *ArchiveJobRequest) Validate() error {
	return validator.New().Struct(jc)
}

//Validate StartJobRequest.
func (jc *StartJobRequest) Validate() error {
	return validator.New().Struct(jc)
}

//Validate StopJobRequest.
func (jc *StopJobRequest) Validate() error {
	return validator.New().Struct(jc)
}
