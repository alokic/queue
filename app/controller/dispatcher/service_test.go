package dispatcher

import (
	"context"
	"github.com/alokic/queue/app/controller"
	"github.com/alokic/queue/app/controller/db"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func seedJobs(t *testing.T, d db.JobDB) {
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
	assert.NoError(t, d.Create(ctx, &job))

	job2 := job
	job2.ID = 102
	job2.Name = "beta"
	assert.NoError(t, d.Create(ctx, &job2))

	job3 := job
	job3.ID = 103
	job3.Name = "gamma"
	assert.NoError(t, d.Create(ctx, &job3))
}

func TestDispatcherService(t *testing.T) {

	log := logger.NewLogrus(logrus.DebugLevel, os.Stdout, map[string]interface{}{})
	d1 := db.NewMockDispatcherDB(log)
	d2 := db.NewMockJobDB(log)
	s := NewDispatcherService(d1, d2, log)
	ctx := context.Background()

	seedJobs(t, d2)

	//Register non existing job
	registerReq := &controller.DispatcherJobRegisterRequest{DispatcherID: 1, JobName: "wow"}
	_, err := s.RegisterJob(ctx, registerReq)
	assert.Error(t, err)

	//Register existing job
	registerReq = &controller.DispatcherJobRegisterRequest{DispatcherID: 1, JobName: "alpha"}
	registerRep, err := s.RegisterJob(ctx, registerReq)
	assert.NoError(t, err)
	assert.Equal(t, registerRep.Job.ID, uint64(101))
	jobs, err := d1.GetJobs(ctx, 1)
	assert.Equal(t, 1, len(jobs))

	registerReq = &controller.DispatcherJobRegisterRequest{DispatcherID: 1, JobName: "beta"}
	registerRep, err = s.RegisterJob(ctx, registerReq)
	assert.NoError(t, err)
	assert.Equal(t, registerRep.Job.ID, uint64(102))
	jobs, err = d1.GetJobs(ctx, 1)
	assert.Equal(t, 2, len(jobs))

	//Sync request for bad dispatcher errs
	syncReq := &controller.DispatcherGossipRequest{DispatcherID: 2}
	_, err = s.Gossip(ctx, syncReq)
	assert.Error(t, err)

	//Sync job for existing dispatcher
	syncReq = &controller.DispatcherGossipRequest{DispatcherID: 1}
	syncRep, err := s.Gossip(ctx, syncReq)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(syncRep.Jobs))

	//Deregister job for bad dispatcher ID
	deregReq := &controller.DispatcherJobDeregisterRequest{DispatcherID: 2, JobID: 1}
	_, err = s.DeregisterJob(ctx, deregReq)
	assert.Error(t, err)

	//Deregister job for bad job ID
	deregReq = &controller.DispatcherJobDeregisterRequest{DispatcherID: 1, JobID: 1000}
	_, err = s.DeregisterJob(ctx, deregReq)
	assert.Error(t, err)

	//Deregister job
	deregReq = &controller.DispatcherJobDeregisterRequest{DispatcherID: 1, JobID: 101}
	_, err = s.DeregisterJob(ctx, deregReq)
	assert.NoError(t, err)
	jobs, err = d1.GetJobs(ctx, 1)
	assert.Equal(t, 1, len(jobs))

	//deregister already deregistered is no-op
	deregReq = &controller.DispatcherJobDeregisterRequest{DispatcherID: 1, JobID: 101}
	_, err = s.DeregisterJob(ctx, deregReq)
	assert.Error(t, err)

	deregReq = &controller.DispatcherJobDeregisterRequest{DispatcherID: 1, JobID: 102}
	_, err = s.DeregisterJob(ctx, deregReq)
	assert.NoError(t, err)
	jobs, err = d1.GetJobs(ctx, 1)
	assert.Error(t, err)
}
