package db

import (
	"context"
	"github.com/alokic/queue/app/controller"
	"github.com/alokic/queue/pkg"
	"github.com/stretchr/testify/assert"
	"testing"
)

/*
Assumes postgres to be running with postgres user and db "queue_test" created.
Also all migrations need to be run.
*/

/*
 Note that we dont test any validation here as thats done at service level.
*/

//func equal(a, b pkg.Job) bool {
//}

func seedJobs(jobsdb JobDB, t *testing.T) {

	ctx := context.Background()

	job := pkg.Job{
		ID:          101,
		Name:        "alpha",
		Description: "beta",
		Type:        pkg.ConsumerJob,
		State:       pkg.Stopped,
		QueueConfig: &pkg.QueueConfig{
			Type:   pkg.KafkaQueue,
			Config: pkg.KafkaConfig{ClusterAddress: []string{"alpha"}, Topic: "alpha"}},
		RetryConfig:       &pkg.RetryConfig{BackOffs: []uint{1}},
		MaxProcessingTime: 10,
		Version:           1,
	}
	assert.NoError(t, jobsdb.Create(ctx, &job))

	job2 := job
	job2.ID = 102
	job2.Name = "beta"
	assert.NoError(t, jobsdb.Create(ctx, &job2))

	job3 := job
	job3.ID = 103
	job3.Name = "gamma"
	assert.NoError(t, jobsdb.Create(ctx, &job3))
}

func TestDispatcherRepo(t *testing.T) {
	jobsdb := NewJobDB(db, log)
	dispdb := NewDispatcherDB(db, log)
	ctx := context.Background()

	//cleanup
	defer func() {
		rawJobs := jobsdb.(*JobRepo)
		rawJobs.jobDB.Exec("DELETE FROM job")
	}()

	//cleanup
	defer func() {
		rawJobs := dispdb.(*DispatcherRepo)
		rawJobs.dispatcherDB.Exec("DELETE FROM dispatcher")
	}()

	rawJobs := jobsdb.(*JobRepo)
	rows, _ := rawJobs.jobDB.Queryx("SELECT id, name, description, type, state, queue_config, retry_config, max_processing_time, version from job")
	for rows.Next() {
		var jobAttrib jobAttrib
		t.Log("ROWS: ", rows.StructScan(&jobAttrib))
	}
	seedJobs(jobsdb, t)

	//Get jobs for non existing dispatcher
	jobs, err := dispdb.GetJobs(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(jobs), 0)

	//Create job now
	j1 := controller.DispatcherJob{ID: 1, DispatcherID: 1, JobID: 101}
	assert.NoError(t, dispdb.CreateJob(ctx, &j1))
	jobs, err = dispdb.GetJobs(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(jobs), 1)
	assert.Equal(t, *jobs[0], j1)

	//Create another job
	j2 := controller.DispatcherJob{ID: 2, DispatcherID: 1, JobID: 102}
	assert.NoError(t, dispdb.CreateJob(ctx, &j2))
	j3 := controller.DispatcherJob{ID: 3, DispatcherID: 1, JobID: 103}
	assert.NoError(t, dispdb.CreateJob(ctx, &j3))
	jobs, err = dispdb.GetJobs(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(jobs), 3)
	assert.Equal(t, *jobs[0], j1)
	assert.Equal(t, *jobs[1], j2)
	assert.Equal(t, *jobs[2], j3)

	//Delete a job j2
	assert.NoError(t, dispdb.DeleteJob(ctx, &j2))
	jobs, err = dispdb.GetJobs(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(jobs), 2)
	assert.Equal(t, *jobs[0], j1)
	assert.Equal(t, *jobs[1], j3)

	//Recreate j2
	assert.NoError(t, dispdb.CreateJob(ctx, &j2))
	jobs, err = dispdb.GetJobs(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(jobs), 3)
	assert.Equal(t, *jobs[0], j1)
	assert.Equal(t, *jobs[1], j3)
	assert.Equal(t, *jobs[2], j2)

	//Delete dispatcher
	assert.NoError(t, dispdb.DeleteDispatcher(ctx, 1))
	jobs, err = dispdb.GetJobs(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(jobs), 0)
}
