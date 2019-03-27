package db

import (
	"context"
	"github.com/alokic/queue/app/controller"
	"github.com/honestbee/gopkg/logger"
	"github.com/pkg/errors"
)

//MockDispatcherRepo struct.
type MockDispatcherRepo struct {
	jobsByID map[uint64]*controller.DispatcherJob
	logger   logger.Logger
}

//NewMockDispatcherDB constructor.
func NewMockDispatcherDB(logger logger.Logger) DispatcherDB {
	return &MockDispatcherRepo{
		logger:   logger,
		jobsByID: make(map[uint64]*controller.DispatcherJob),
	}
}

//GetJobs for getting a dispatcher job.
func (jr *MockDispatcherRepo) GetJobs(ctx context.Context, dispID uint64) ([]*controller.DispatcherJob, error) {
	jobs := []*controller.DispatcherJob{}
	for _, j := range jr.jobsByID {
		if j.DispatcherID == dispID {
			jobs = append(jobs, j)
		}
	}
	if len(jobs) == 0 {
		return nil, errors.New("no jobs found")
	}
	return jobs, nil
}

//CreateJob for creating a job.
func (jr *MockDispatcherRepo) CreateJob(ctx context.Context, j *controller.DispatcherJob) error {
	jr.jobsByID[j.ID] = j
	return nil
}

//DeleteJob for deleting a job.
func (jr *MockDispatcherRepo) DeleteJob(ctx context.Context, j *controller.DispatcherJob) error {
	var job *controller.DispatcherJob
	for _, e := range jr.jobsByID {
		if j.DispatcherID == e.DispatcherID && j.JobID == e.JobID {
			job = e
			break
		}
	}
	if job == nil {
		return errors.New("job not found")
	}

	delete(jr.jobsByID, job.ID)
	return nil
}

//DeleteDispatcher func.
func (jr *MockDispatcherRepo) DeleteDispatcher(ctx context.Context, dispID uint64) error {
	jobs, err := jr.GetJobs(ctx, dispID)
	if err != nil {
		return err
	}

	for _, j := range jobs {
		delete(jr.jobsByID, j.ID)
	}
	return nil
}
