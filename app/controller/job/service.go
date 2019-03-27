package job

import (
	"context"
	"github.com/alokic/queue/app/controller"
	"github.com/alokic/queue/app/controller/db"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/pkg/errors"
)

type (
	jobService struct {
		jobDB  db.JobDB
		logger logger.Logger
	}
)

const (
	name = "name"
)

//New constructor.
func New(jobDB db.JobDB, logger logger.Logger) controller.JobService {
	lg := logger.ContextualLogger(map[string]interface{}{"context": "job-service"})
	return &jobService{jobDB: jobDB, logger: lg}
}

func (jcs *jobService) GetJob(ctx context.Context) (*controller.GetJobReply, error) {
	name, _ := ctx.Value(name).(string)
	if name == "" {
		return nil, errors.New("unable to get job name")
	}

	job, err := jcs.jobDB.Get(ctx, name)
	if err != nil {
		return nil, err
	}

	reply := new(controller.GetJobReply)
	reply.Job = *job
	return reply, nil
}

func (jcs *jobService) CreateJob(ctx context.Context, in *controller.CreateJobRequest) (*controller.CreateJobReply, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}

	jc := new(pkg.Job)

	jc.ID = pkg.MustGid(5)
	jc.Name = in.Name
	jc.Description = in.Description
	jc.Type = in.Type
	jc.State = pkg.Stopped
	jc.QueueConfig = &in.QueueConfig
	jc.RetryConfig = in.RetryConfig
	jc.MaxProcessingTime = in.MaxProcessingTime
	jc.Version = 1

	err := jcs.jobDB.Create(ctx, jc)
	if err != nil {
		return nil, err
	}

	reply := new(controller.CreateJobReply)
	return reply, nil
}

func (jcs *jobService) UpdateJob(ctx context.Context, in *controller.UpdateJobRequest) (*controller.UpdateJobReply, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}

	name, _ := ctx.Value(name).(string)
	if name == "" {
		return nil, errors.New("unable to get job name")
	}

	jc := new(pkg.Job)
	jc.ID = pkg.MustGid(5)
	jc.Name = name
	jc.Description = in.Description
	jc.MaxProcessingTime = in.MaxProcessingTime
	jc.RetryConfig = in.RetryConfig
	jc.QueueConfig = in.QueueConfig

	err := jcs.jobDB.ArchivingUpdate(ctx, jc)
	if err != nil {
		return nil, err
	}

	reply := new(controller.UpdateJobReply)
	return reply, nil
}

func (jcs *jobService) ArchiveJob(ctx context.Context, in *controller.ArchiveJobRequest) (*controller.ArchiveJobReply, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}

	name, _ := ctx.Value(name).(string)
	if name == "" {
		return nil, errors.New("unable to get job name")
	}

	if err := jcs.jobDB.Archive(ctx, name); err != nil {
		return nil, err
	}

	reply := new(controller.ArchiveJobReply)
	return reply, nil
}

func (jcs *jobService) StartJob(ctx context.Context, in *controller.StartJobRequest) (*controller.StartJobReply, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}

	name, _ := ctx.Value(name).(string)
	if name == "" {
		return nil, errors.New("unable to get job name")
	}

	jc := new(pkg.Job)
	jc.Name = name
	jc.State = pkg.Active

	err := jcs.jobDB.Update(ctx, jc)
	if err != nil {
		return nil, err
	}

	reply := new(controller.StartJobReply)
	return reply, nil
}

func (jcs *jobService) StopJob(ctx context.Context, in *controller.StopJobRequest) (*controller.StopJobReply, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}

	name, _ := ctx.Value(name).(string)
	if name == "" {
		return nil, errors.New("unable to get job name")
	}

	jc := new(pkg.Job)
	jc.Name = name
	jc.State = pkg.Stopped

	err := jcs.jobDB.Update(ctx, jc)
	if err != nil {
		return nil, err
	}

	reply := new(controller.StopJobReply)
	return reply, nil
}
