package db

import (
	"context"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/pkg/errors"
)

//MockJobRepo struct.
type MockJobRepo struct {
	jobsByID map[uint64]*pkg.Job
	logger   logger.Logger
}

//NewMockJobDB constructor.
func NewMockJobDB(logger logger.Logger) JobDB {
	return &MockJobRepo{
		logger:   logger,
		jobsByID: make(map[uint64]*pkg.Job),
	}
}

//Get for getting a ConsumerWorklet.
func (jr *MockJobRepo) Get(ctx context.Context, name string) (*pkg.Job, error) {
	for _, j := range jr.jobsByID {
		if j.Name == name && (j.State == pkg.Active || j.State == pkg.Stopped) {
			return j, nil
		}
	}
	return nil, errors.New("not found")
}

//GetMany for getting all jobs.
func (jr *MockJobRepo) GetMany(ctx context.Context, ids []uint64) ([]*pkg.Job, error) {
	jobs := []*pkg.Job{}

	for _, id := range ids {
		job := jr.jobsByID[id]
		if job != nil {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

//Create for creating consumer job.
func (jr *MockJobRepo) Create(ctx context.Context, in *pkg.Job) error {
	jr.jobsByID[in.ID] = in
	return nil
}

//Update for updating existing job in-place.
func (jr *MockJobRepo) Update(ctx context.Context, newJob *pkg.Job) error {
	oldJob, err := jr.Get(ctx, newJob.Name)
	if err != nil {
		return err
	}
	mJob := mergeJob(*oldJob, *newJob)
	jr.jobsByID[mJob.ID] = &mJob
	return nil
}

//ArchivingUpdate deprecates old version and creates a new version.
func (jr *MockJobRepo) ArchivingUpdate(ctx context.Context, newJob *pkg.Job) error {
	oldJob, err := jr.Get(ctx, newJob.Name)
	if err != nil {
		return err
	}
	mJob := mergeJob(*oldJob, *newJob)
	oldJob.State = pkg.Deprecated
	jr.jobsByID[oldJob.ID] = oldJob

	mJob.Version = oldJob.Version + 1
	jr.jobsByID[mJob.ID] = &mJob
	jr.logJobs()
	return nil
}

//Archive marks as deleted.
func (jr *MockJobRepo) Archive(ctx context.Context, name string) error {
	oldJob, err := jr.Get(ctx, name)
	if err != nil {
		return err
	}
	oldJob.State = pkg.Archived
	jr.jobsByID[oldJob.ID] = oldJob
	return nil
}

//HealthCheck checks for jobDB up.
func (jr *MockJobRepo) HealthCheck(ctx context.Context) error {
	return nil
}

func (jr *MockJobRepo) logJobs() {
	for id, job := range jr.jobsByID {
		jr.logger.Infof("%v -> %v", id, *job)
	}
}

//mergeJob merges b into a.
func mergeJob(a, b pkg.Job) pkg.Job {
	if b.ID != 0 {
		a.ID = b.ID
	}

	if b.Name != "" {
		a.Name = b.Name
	}

	if b.Description != "" {
		a.Description = b.Description
	}

	if b.Type != "" {
		a.Type = b.Type
	}

	if b.State != "" {
		a.State = b.State
	}

	if b.QueueConfig != nil {
		a.QueueConfig = b.QueueConfig
	}

	if b.RetryConfig != nil {
		a.RetryConfig = b.RetryConfig
	}

	if b.MaxProcessingTime != 0 {
		a.MaxProcessingTime = b.MaxProcessingTime
	}

	if b.Version != 0 {
		a.Version = b.Version
	}

	return a
}
