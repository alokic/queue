package cache

import (
	"context"
	"github.com/alokic/queue/app/dispatcher"
	"github.com/alokic/queue/pkg"
)

//JobRepo is repo of jobs.
type JobRepo struct {
	Map *Map
}

//NewJobRepo creates new JobRepo.
func NewJobRepo() dispatcher.JobRepository {
	return &JobRepo{
		Map: NewMap(),
	}
}

//Put creates or updates a job.
func (jr *JobRepo) Put(ctx context.Context, k string, job *pkg.Job) {
	jr.Map.Set(job.Name, job)
}

//Get gets a job.
func (jr *JobRepo) Get(ctx context.Context, k string) *pkg.Job {
	v := jr.Map.Get(k)
	if v == nil {
		return nil
	}
	job, ok := v.(*pkg.Job)
	if !ok {
		return nil
	}
	return job
}

//Delete deletes a job.
func (jr *JobRepo) Delete(ctx context.Context, k string) {
	jr.Map.Delete(k)
}
