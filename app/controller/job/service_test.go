package job

import (
	"context"
	"encoding/json"
	"github.com/alokic/queue/app/controller"
	db2 "github.com/alokic/queue/app/controller/db"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"strings"
	"testing"
)

func strfromList(a []uint) string {
	res := []string{}
	for _, e := range a {
		res = append(res, strconv.FormatUint(uint64(e), 10))
	}
	return strings.Join(res, ",")
}

func equalCreate(a controller.CreateJobRequest, b pkg.Job) bool {
	if a.Name != b.Name || a.Type != b.Type ||
		a.Description != b.Description || a.MaxProcessingTime != b.MaxProcessingTime {
		return false
	}

	if b.QueueConfig == nil {
		return false
	}
	if a.QueueConfig.Type != b.QueueConfig.Type {
		return false
	}
	a1, _ := json.Marshal(a.QueueConfig.Config)
	b1, _ := json.Marshal(b.QueueConfig.Config)
	if string(a1) != string(b1) {
		return false
	}

	if a.RetryConfig == nil {
		if b.RetryConfig != nil {
			return false
		}
	} else {
		if b.RetryConfig == nil {
			return false
		}
		if strfromList(a.RetryConfig.BackOffs) != strfromList(b.RetryConfig.BackOffs) {
			return false
		}
	}
	return true
}

func equalUpdate(a controller.UpdateJobRequest, b pkg.Job) bool {
	if a.Description != b.Description || a.MaxProcessingTime != b.MaxProcessingTime {
		return false
	}

	if b.QueueConfig == nil {
		return false
	}
	if a.QueueConfig.Type != b.QueueConfig.Type {
		return false
	}
	a1, _ := json.Marshal(a.QueueConfig.Config)
	b1, _ := json.Marshal(b.QueueConfig.Config)
	if string(a1) != string(b1) {
		return false
	}

	if a.RetryConfig == nil {
		if b.RetryConfig != nil {
			return false
		}
	} else {
		if b.RetryConfig == nil {
			return false
		}
		if strfromList(a.RetryConfig.BackOffs) != strfromList(b.RetryConfig.BackOffs) {
			return false
		}
	}
	return true
}
func TestJobService(t *testing.T) {
	log := logger.NewLogrus(logrus.DebugLevel, os.Stdout, map[string]interface{}{})
	d := db2.NewMockJobDB(log)
	s := New(d, log)
	ctx := context.Background()

	//No JOB
	_, err := s.GetJob(ctx)
	assert.Error(t, err)

	//CREATE JOB
	cr1 := &controller.CreateJobRequest{
		Name:        "alpha",
		Description: "beta",
		Type:        pkg.ConsumerJob,
		QueueConfig: pkg.QueueConfig{
			Type:   pkg.KafkaQueue,
			Config: pkg.KafkaConfig{ClusterAddress: []string{"alpha"}, Topic: "alpha"}},
		RetryConfig:       &pkg.RetryConfig{BackOffs: []uint{1}},
		MaxProcessingTime: 10,
	}
	_, err = s.CreateJob(ctx, cr1)
	assert.NoError(t, err)

	//Get wrong JOB
	ctx2 := context.WithValue(ctx, "name", "test")
	_, err = s.GetJob(ctx2)
	assert.Error(t, err)

	//Get right JOB
	ctx3 := context.WithValue(ctx, "name", "alpha")
	rep, err := s.GetJob(ctx3)
	assert.NoError(t, err)
	assert.True(t, equalCreate(*cr1, rep.Job))
	assert.Equal(t, rep.Job.Version, 1)
	assert.Equal(t, rep.Job.State, pkg.Stopped)

	//Bad Update Job
	ur1 := &controller.UpdateJobRequest{
		Description: "wow",
		QueueConfig: &pkg.QueueConfig{
			Type:   pkg.KafkaQueue,
			Config: pkg.KafkaConfig{ClusterAddress: []string{"wow"}, Topic: "wow"}},
		RetryConfig:       &pkg.RetryConfig{BackOffs: []uint{2}},
		MaxProcessingTime: 11,
	}
	_, err = s.UpdateJob(ctx, ur1)
	assert.Error(t, err)
	_, err = s.UpdateJob(ctx2, ur1)
	assert.Error(t, err)

	//Update works
	_, err = s.UpdateJob(ctx3, ur1)
	assert.NoError(t, err)
	rep, err = s.GetJob(ctx3)
	assert.NoError(t, err)
	assert.True(t, equalUpdate(*ur1, rep.Job))
	assert.Equal(t, rep.Job.Version, 2) //version increased
	assert.Equal(t, rep.Job.State, pkg.Stopped)

	//Start bad job
	sj1 := &controller.StartJobRequest{}
	_, err = s.StartJob(ctx, sj1)
	assert.Error(t, err)

	_, err = s.StartJob(ctx2, sj1)
	assert.Error(t, err)

	//Start stopped job
	_, err = s.StartJob(ctx3, sj1)
	assert.NoError(t, err)
	rep, err = s.GetJob(ctx3)
	assert.NoError(t, err)
	assert.Equal(t, rep.Job.Version, 2) //version increased
	assert.Equal(t, rep.Job.State, pkg.Active)

	//Starting started job is no-op
	_, err = s.StartJob(ctx3, sj1)
	assert.NoError(t, err)
	rep, err = s.GetJob(ctx3)
	assert.NoError(t, err)
	assert.Equal(t, rep.Job.Version, 2) //version increased
	assert.Equal(t, rep.Job.State, pkg.Active)

	//Stop bad job
	sj2 := &controller.StopJobRequest{}
	_, err = s.StopJob(ctx, sj2)
	assert.Error(t, err)

	_, err = s.StopJob(ctx2, sj2)
	assert.Error(t, err)

	//Stop started job
	_, err = s.StopJob(ctx3, sj2)
	assert.NoError(t, err)
	rep, err = s.GetJob(ctx3)
	assert.NoError(t, err)
	assert.Equal(t, rep.Job.Version, 2) //version increased
	assert.Equal(t, rep.Job.State, pkg.Stopped)

	//Stopping stopped job is no-op
	_, err = s.StopJob(ctx3, sj2)
	assert.NoError(t, err)
	rep, err = s.GetJob(ctx3)
	assert.NoError(t, err)
	assert.Equal(t, rep.Job.Version, 2) //version increased
	assert.Equal(t, rep.Job.State, pkg.Stopped)

	//Archive a bad JOB
	aj := &controller.ArchiveJobRequest{}
	_, err = s.ArchiveJob(ctx, aj)
	assert.Error(t, err)

	_, err = s.ArchiveJob(ctx2, aj)
	assert.Error(t, err)

	//Archive a good job
	_, err = s.ArchiveJob(ctx3, aj)
	assert.NoError(t, err)
	_, err = s.GetJob(ctx3)
	assert.Error(t, err)
}
