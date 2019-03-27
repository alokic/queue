package dispatcher

import (
	"context"
	"github.com/alokic/queue/app/controller"
	"github.com/alokic/queue/app/controller/db"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/pkg/errors"
)

type (
	dispatcherService struct {
		dispatcherDB db.DispatcherDB
		jobDB        db.JobDB
		logger       logger.Logger
	}
)

//NewDispatcherService constructor.
func NewDispatcherService(dispatcherDB db.DispatcherDB, jobDB db.JobDB, logger logger.Logger) controller.DispatcherService {
	lg := logger.ContextualLogger(map[string]interface{}{"context": "dispatcher-service"})
	return &dispatcherService{dispatcherDB: dispatcherDB, jobDB: jobDB, logger: lg}
}

//RegisterJob registers a job.
func (ds *dispatcherService) RegisterJob(ctx context.Context, in *controller.DispatcherJobRegisterRequest) (*controller.DispatcherJobRegisterReply, error) {
	if err := in.Validate(); err != nil {
		return nil, pkg.ErrBadRequest{Err: err}
	}

	job, err := ds.jobDB.Get(ctx, in.JobName)
	if err != nil {
		switch err.(type) {
		case pkg.ErrNotFound:
			return nil, pkg.ErrNotFound{Err: err}
		default:
			return nil, pkg.ErrInternalServerError{Err: errors.Wrap(err, "error in getting job from DB")}
		}
	}

	dj := new(controller.DispatcherJob)
	dj.ID = pkg.MustGid(5)
	dj.JobID = job.ID
	dj.DispatcherID = in.DispatcherID
	if err := ds.dispatcherDB.CreateJob(ctx, dj); err != nil {
		return nil, pkg.ErrInternalServerError{Err: errors.Wrap(err, "error in creating dispatcher entry in DB")}
	}

	reply := new(controller.DispatcherJobRegisterReply)
	reply.Job = job

	return reply, nil
}

//DeregisterJob deregisters a job.
func (ds *dispatcherService) DeregisterJob(ctx context.Context, in *controller.DispatcherJobDeregisterRequest) (*controller.DispatcherJobDeregisterReply, error) {
	if err := in.Validate(); err != nil {
		return nil, pkg.ErrBadRequest{Err: err}
	}

	dj := new(controller.DispatcherJob)
	dj.ID = pkg.MustGid(5)
	dj.JobID = in.JobID
	dj.DispatcherID = in.DispatcherID
	if err := ds.dispatcherDB.DeleteJob(ctx, dj); err != nil {
		return nil, pkg.ErrInternalServerError{Err: err}
	}

	reply := new(controller.DispatcherJobDeregisterReply)

	return reply, nil
}

//Gossip gets all jobs for dispatcher.
func (ds *dispatcherService) Gossip(ctx context.Context, in *controller.DispatcherGossipRequest) (*controller.DispatcherGossipReply, error) {
	if err := in.Validate(); err != nil {
		return nil, pkg.ErrBadRequest{Err: err}
	}

	dispatcherJobs, err := ds.dispatcherDB.GetJobs(ctx, in.DispatcherID)
	if err != nil {
		return nil, errors.Wrap(err, "error in getting job ids")
	}

	jobIDs := []uint64{}
	for _, d := range dispatcherJobs {
		jobIDs = append(jobIDs, d.JobID)
	}

	jobs := []*pkg.Job{}

	if len(jobIDs) > 0 {
		jobs, err = ds.jobDB.GetMany(ctx, jobIDs)
		if err != nil {
			return nil, errors.Wrap(err, "error in getting jobs")
		}
	}
	reply := new(controller.DispatcherGossipReply)
	reply.Jobs = jobs
	return reply, nil
}
