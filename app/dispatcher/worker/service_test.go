package worker

import (
	"context"
	"fmt"
	"github.com/alokic/queue/app/dispatcher"
	"github.com/alokic/queue/app/dispatcher/cache"
	"github.com/alokic/queue/app/dispatcher/queue"
	"github.com/alokic/queue/app/dispatcher/worklet"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"
)

var mockedGetRetryPair = func(j *pkg.Job, nativeQueueAdd string, logger logger.Logger) ([]queue.Consumer, []queue.Producer) {
	retryProducers := []queue.Producer{}
	retryConsumers := []queue.Consumer{}

	for i := 0; i < len(j.RetryConfig.BackOffs); i++ {
		retryProducers = append(retryProducers, &queue.MockProducer{})
		retryConsumers = append(retryConsumers, &queue.MockConsumer{})
	}

	return retryConsumers, retryProducers
}

var mockedGetErrRetryPair = func(j *pkg.Job, nativeQueueAdd string, logger logger.Logger) ([]queue.Consumer, []queue.Producer) {
	retryProducers := []queue.Producer{}
	retryConsumers := []queue.Consumer{}

	for i := 0; i < len(j.RetryConfig.BackOffs); i++ {
		retryProducers = append(retryProducers, &queue.MockProducer{FailInit: true})
		retryConsumers = append(retryConsumers, &queue.MockConsumer{})
	}

	return retryConsumers, retryProducers
}

var mockedGetDLQPair = func(j *pkg.Job, nativeQueueAdd string, logger logger.Logger) (queue.Consumer, queue.Producer) {
	dlqProducer := &queue.MockProducer{}
	dlqConsumer := &queue.MockConsumer{}

	return dlqConsumer, dlqProducer
}

var mockedGetAckMgr = func(j *pkg.Job, nativeQueueAdd string, logger logger.Logger) queue.Producer {
	dlqProducer := &queue.MockProducer{}

	return dlqProducer
}

func TestService_SubscribeJob(t *testing.T) {
	oldgetRetryPair := getRetryPair //mocked so that we dont call kafka actually
	oldgetDLQPair := getDLQPair
	oldGetAckMgr := getAckMgr
	defer func() {
		getRetryPair = oldgetRetryPair
		getDLQPair = oldgetDLQPair
		getAckMgr = oldGetAckMgr
	}()
	getRetryPair = mockedGetRetryPair
	getDLQPair = mockedGetDLQPair
	getAckMgr = mockedGetAckMgr

	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
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
	mockController := &MockController{
		Job: &job,
		Mu:  sync.RWMutex{},
	}
	consumerRepo := cache.NewConsumerRepo()
	producerRepo := cache.NewProducerRepo()
	jobRepo := cache.NewJobRepo()
	s := NewService(123, mockController, jobRepo, consumerRepo, producerRepo, "alpha:8113", time.Second, logger)
	rawS := s.(*service)

	t.Run("bad request", func(t *testing.T) {
		req := &dispatcher.WorkerSubscribeJobRequest{}
		rep, err := s.SubscribeJob(context.Background(), req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("context done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req := &dispatcher.WorkerSubscribeJobRequest{}
		rep, err := s.SubscribeJob(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("job already subscribed", func(t *testing.T) {
		defer jobRepo.Delete(context.Background(), job.Name)
		jobRepo.Put(context.Background(), job.Name, &job)
		req := &dispatcher.WorkerSubscribeJobRequest{Name: job.Name}
		rep, err := s.SubscribeJob(context.Background(), req)
		assert.NoError(t, err)
		assert.NotNil(t, rep)
	})

	t.Run("controller errs", func(t *testing.T) {
		defer jobRepo.Delete(context.Background(), job.Name)
		mockController.GossipErr = true
		req := &dispatcher.WorkerSubscribeJobRequest{Name: job.Name}
		rep, err := s.SubscribeJob(context.Background(), req)
		t.Log(err)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("bad job type", func(t *testing.T) {
		fmt.Println(t.Name())
		mockController.Job.Type = pkg.JobType("alpha_jobtype")
		defer func() {
			mockController.Job.Type = pkg.ConsumerJob
		}()
		mockController.GossipErr = false
		req := &dispatcher.WorkerSubscribeJobRequest{Name: job.Name}
		rep, err := s.SubscribeJob(context.Background(), req)
		t.Log(err)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "bad job type")
		assert.Nil(t, rep)
	})

	t.Run("bad queue type", func(t *testing.T) {
		fmt.Println(t.Name())
		mockController.Job.QueueConfig.Type = pkg.QueueType("alpha_queuetype")
		defer func() {
			mockController.Job.QueueConfig.Type = pkg.KafkaQueue
		}()
		mockController.GossipErr = false
		req := &dispatcher.WorkerSubscribeJobRequest{Name: job.Name}
		rep, err := s.SubscribeJob(context.Background(), req)
		t.Log(err)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "bad queue type")
		assert.Nil(t, rep)
	})

	t.Run("bad queue config", func(t *testing.T) {
		fmt.Println(t.Name())
		mockController.Job.QueueConfig.Type = pkg.KafkaQueue
		mockController.Job.QueueConfig.Config = "ww"
		defer func() {
			mockController.Job.QueueConfig.Config = pkg.KafkaConfig{ClusterAddress: []string{"alpha"}, Topic: "alpha"}
		}()

		mockController.GossipErr = false
		req := &dispatcher.WorkerSubscribeJobRequest{Name: job.Name}
		rep, err := s.SubscribeJob(context.Background(), req)
		t.Log(err)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "bad kafka config")
		assert.Nil(t, rep)
	})

	t.Run("failed worklet init", func(t *testing.T) {
		getRetryPair = mockedGetErrRetryPair
		defer func() {
			getRetryPair = mockedGetRetryPair
		}()
		mockController.GossipErr = false
		req := &dispatcher.WorkerSubscribeJobRequest{Name: job.Name}
		rep, err := s.SubscribeJob(context.Background(), req)
		t.Log(err)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("all good", func(t *testing.T) {
		t.Log(mockController.Job.QueueConfig)
		mockController.GossipErr = false
		req := &dispatcher.WorkerSubscribeJobRequest{Name: job.Name}
		rep, err := s.SubscribeJob(context.Background(), req)
		assert.NoError(t, err)
		assert.NotNil(t, rep)
		assert.NotNil(t, rawS.jobsRepo.Get(context.Background(), job.Name))
		assert.NotNil(t, rawS.consumerRepo.Get(context.Background(), job.Name))
	})
}

func TestService_UnsubscribeJob(t *testing.T) {
	oldgetRetryPair := getRetryPair //mocked so that we dont call kafka actually
	oldgetDLQPair := getDLQPair
	oldGetAckMgr := getAckMgr
	defer func() {
		getRetryPair = oldgetRetryPair
		getDLQPair = oldgetDLQPair
		getAckMgr = oldGetAckMgr
	}()
	getRetryPair = mockedGetRetryPair
	getDLQPair = mockedGetDLQPair
	getAckMgr = mockedGetAckMgr

	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	job := &pkg.Job{
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
	mockController := &MockController{
		Job: job,
		Mu:  sync.RWMutex{},
	}
	consumerRepo := cache.NewConsumerRepo()
	producerRepo := cache.NewProducerRepo()
	jobRepo := cache.NewJobRepo()
	s := NewService(123, mockController, jobRepo, consumerRepo, producerRepo, "alpha:8113", time.Second, logger)
	rawS := s.(*service)

	t.Run("bad request", func(t *testing.T) {
		req := &dispatcher.WorkerUnsubscribeJobRequest{}
		rep, err := s.UnsubscribeJob(context.Background(), req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("context done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req := &dispatcher.WorkerUnsubscribeJobRequest{}
		rep, err := s.UnsubscribeJob(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("job already unsubscribed", func(t *testing.T) {
		jobRepo.Delete(context.Background(), job.Name)
		req := &dispatcher.WorkerUnsubscribeJobRequest{Name: job.Name}
		rep, err := s.UnsubscribeJob(context.Background(), req)
		assert.NoError(t, err)
		assert.NotNil(t, rep)
	})

	t.Run("controller errs", func(t *testing.T) {
		defer jobRepo.Delete(context.Background(), job.Name)
		jobRepo.Put(context.Background(), job.Name, job)
		mockController.GossipErr = true
		req := &dispatcher.WorkerUnsubscribeJobRequest{Name: job.Name}
		rep, err := s.UnsubscribeJob(context.Background(), req)
		t.Log(err)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("bad job type", func(t *testing.T) {
		badJob := *job
		badJob.Type = pkg.JobType("alpha")
		mockController.Job = &badJob
		jobRepo.Put(context.Background(), job.Name, &badJob)
		defer jobRepo.Delete(context.Background(), badJob.Name)
		defer func() {
			mockController.Job = job
		}()
		mockController.GossipErr = false
		req := &dispatcher.WorkerUnsubscribeJobRequest{Name: job.Name}
		rep, err := s.UnsubscribeJob(context.Background(), req)
		t.Log(err)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "bad job type")
		assert.Nil(t, rep)
	})

	t.Run("job not found", func(t *testing.T) {
		jobRepo.Put(context.Background(), job.Name, job)
		defer jobRepo.Delete(context.Background(), job.Name)
		mockController.GossipErr = false
		req := &dispatcher.WorkerUnsubscribeJobRequest{Name: job.Name}
		rep, err := s.UnsubscribeJob(context.Background(), req)
		t.Log(err)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no consumer worklet found")
		assert.Nil(t, rep)
	})

	t.Run("all good", func(t *testing.T) {
		mockController.GossipErr = false
		req1 := &dispatcher.WorkerSubscribeJobRequest{Name: job.Name}
		_, err := s.SubscribeJob(context.Background(), req1)
		assert.NoError(t, err)
		assert.NotNil(t, rawS.jobsRepo.Get(context.Background(), job.Name))
		assert.NotNil(t, rawS.consumerRepo.Get(context.Background(), job.Name))

		req := &dispatcher.WorkerUnsubscribeJobRequest{Name: job.Name}
		rep, err := s.UnsubscribeJob(context.Background(), req)
		t.Log(err)
		assert.NoError(t, err)
		assert.NotNil(t, rep)
		assert.Nil(t, rawS.jobsRepo.Get(context.Background(), job.Name))
		assert.Nil(t, rawS.consumerRepo.Get(context.Background(), job.Name))

	})
}

func TestService_GetMsgs(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	job := &pkg.Job{
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
	mockController := &MockController{
		Job: job,
		Mu:  sync.RWMutex{},
	}
	consumerRepo := cache.NewConsumerRepo()
	producerRepo := cache.NewProducerRepo()
	jobRepo := cache.NewJobRepo()
	s := NewService(123, mockController, jobRepo, consumerRepo, producerRepo, "alpha:8113", time.Second, logger)
	rawS := s.(*service)
	t.Run("bad request", func(t *testing.T) {
		req := &dispatcher.WorkerGetMsgsRequest{}
		rep, err := s.GetMsgs(context.Background(), req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("context done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req := &dispatcher.WorkerGetMsgsRequest{}
		rep, err := s.GetMsgs(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("job not found", func(t *testing.T) {
		req := &dispatcher.WorkerGetMsgsRequest{Name: job.Name, MaxWait: 100}
		rep, err := s.GetMsgs(context.Background(), req)
		assert.Error(t, err)
		assert.NotNil(t, err.Error(), "no consumer worklet found")
		assert.Nil(t, rep)
	})

	t.Run("job not found", func(t *testing.T) {
		con := worklet.MockConsumer{}
		con.ShouldErr = true
		rawS.consumerRepo.Put(context.Background(), job.Name, &con)
		defer rawS.consumerRepo.Delete(context.Background(), job.Name)

		req := &dispatcher.WorkerGetMsgsRequest{Name: job.Name, MaxWait: 100}
		rep, err := s.GetMsgs(context.Background(), req)
		assert.Error(t, err)
		t.Log(err)
		assert.Nil(t, rep)
	})

	t.Run("all good", func(t *testing.T) {
		con := worklet.MockConsumer{}
		con.ShouldErr = false
		con.Message = pkg.Message{}
		rawS.consumerRepo.Put(context.Background(), job.Name, &con)
		defer rawS.consumerRepo.Delete(context.Background(), job.Name)

		req := &dispatcher.WorkerGetMsgsRequest{Name: job.Name, MaxWait: 100}
		rep, err := s.GetMsgs(context.Background(), req)
		assert.NoError(t, err)
		assert.NotNil(t, rep)
	})
}

func TestService_GetRetryMsgs(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	job := &pkg.Job{
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
	mockController := &MockController{
		Job: job,
		Mu:  sync.RWMutex{},
	}
	consumerRepo := cache.NewConsumerRepo()
	producerRepo := cache.NewProducerRepo()
	jobRepo := cache.NewJobRepo()
	s := NewService(123, mockController, jobRepo, consumerRepo, producerRepo, "alpha:8113", time.Second, logger)
	rawS := s.(*service)
	t.Run("bad request", func(t *testing.T) {
		req := &dispatcher.WorkerGetRetryMsgsRequest{}
		rep, err := s.GetRetryMsgs(context.Background(), req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("context done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req := &dispatcher.WorkerGetRetryMsgsRequest{}
		rep, err := s.GetRetryMsgs(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("job not found", func(t *testing.T) {
		req := &dispatcher.WorkerGetRetryMsgsRequest{Name: job.Name, MaxWait: 100}
		rep, err := s.GetRetryMsgs(context.Background(), req)
		assert.Error(t, err)
		assert.NotNil(t, err.Error(), "no consumer worklet found")
		assert.Nil(t, rep)
	})

	t.Run("consumer errs", func(t *testing.T) {
		con := worklet.MockConsumer{}
		con.ShouldErr = true
		rawS.consumerRepo.Put(context.Background(), job.Name, &con)
		defer rawS.consumerRepo.Delete(context.Background(), job.Name)

		req := &dispatcher.WorkerGetRetryMsgsRequest{Name: job.Name, MaxWait: 100}
		rep, err := s.GetRetryMsgs(context.Background(), req)
		assert.Error(t, err)
		t.Log(err)
		assert.Nil(t, rep)
	})

	t.Run("all good", func(t *testing.T) {
		con := worklet.MockConsumer{}
		con.ShouldErr = false
		con.Message = pkg.Message{}
		rawS.consumerRepo.Put(context.Background(), job.Name, &con)
		defer rawS.consumerRepo.Delete(context.Background(), job.Name)

		req := &dispatcher.WorkerGetRetryMsgsRequest{Name: job.Name, MaxWait: 100}
		rep, err := s.GetRetryMsgs(context.Background(), req)
		assert.NoError(t, err)
		assert.NotNil(t, rep)
	})
}

func TestService_GetDLQMsgs(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	job := &pkg.Job{
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
	mockController := &MockController{
		Job: job,
		Mu:  sync.RWMutex{},
	}
	consumerRepo := cache.NewConsumerRepo()
	producerRepo := cache.NewProducerRepo()
	jobRepo := cache.NewJobRepo()
	s := NewService(123, mockController, jobRepo, consumerRepo, producerRepo, "alpha:8113", time.Second, logger)
	rawS := s.(*service)
	t.Run("bad request", func(t *testing.T) {
		req := &dispatcher.WorkerGetDLQMsgsRequest{}
		rep, err := s.GetDLQMsgs(context.Background(), req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("context done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req := &dispatcher.WorkerGetDLQMsgsRequest{}
		rep, err := s.GetDLQMsgs(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("job not found", func(t *testing.T) {
		req := &dispatcher.WorkerGetDLQMsgsRequest{Name: job.Name, MaxWait: 100}
		rep, err := s.GetDLQMsgs(context.Background(), req)
		assert.Error(t, err)
		assert.NotNil(t, err.Error(), "no consumer worklet found")
		assert.Nil(t, rep)
	})

	t.Run("consumer errs", func(t *testing.T) {
		con := worklet.MockConsumer{}
		con.ShouldErr = true
		rawS.consumerRepo.Put(context.Background(), job.Name, &con)
		defer rawS.consumerRepo.Delete(context.Background(), job.Name)

		req := &dispatcher.WorkerGetDLQMsgsRequest{Name: job.Name, MaxWait: 100}
		rep, err := s.GetDLQMsgs(context.Background(), req)
		assert.Error(t, err)
		t.Log(err)
		assert.Nil(t, rep)
	})

	t.Run("all good", func(t *testing.T) {
		con := worklet.MockConsumer{}
		con.ShouldErr = false
		con.Message = pkg.Message{}
		rawS.consumerRepo.Put(context.Background(), job.Name, &con)
		defer rawS.consumerRepo.Delete(context.Background(), job.Name)

		req := &dispatcher.WorkerGetDLQMsgsRequest{Name: job.Name, MaxWait: 100}
		rep, err := s.GetDLQMsgs(context.Background(), req)
		assert.NoError(t, err)
		assert.NotNil(t, rep)
	})
}

func TestService_AckMsgsMsgs(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	job := &pkg.Job{
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
	mockController := &MockController{
		Job: job,
		Mu:  sync.RWMutex{},
	}
	consumerRepo := cache.NewConsumerRepo()
	producerRepo := cache.NewProducerRepo()
	jobRepo := cache.NewJobRepo()
	s := NewService(123, mockController, jobRepo, consumerRepo, producerRepo, "alpha:8113", time.Second, logger)
	rawS := s.(*service)

	t.Run("bad request", func(t *testing.T) {
		req := &dispatcher.WorkerAckMsgsRequest{}
		rep, err := s.AckMsgs(context.Background(), req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("context done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req := &dispatcher.WorkerAckMsgsRequest{}
		rep, err := s.AckMsgs(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("worker not found", func(t *testing.T) {
		req := &dispatcher.WorkerAckMsgsRequest{Name: job.Name, MsgIDs: []uint64{1}}
		rep, err := s.AckMsgs(context.Background(), req)
		assert.Error(t, err)
		assert.NotNil(t, err.Error(), "no consumer worklet found")
		assert.Nil(t, rep)
	})

	t.Run("consumer errs", func(t *testing.T) {
		con := worklet.MockConsumer{}
		con.ShouldErr = true
		rawS.consumerRepo.Put(context.Background(), job.Name, &con)
		defer rawS.consumerRepo.Delete(context.Background(), job.Name)

		req := &dispatcher.WorkerAckMsgsRequest{Name: job.Name, MsgIDs: []uint64{1}}
		rep, err := s.AckMsgs(context.Background(), req)
		assert.Error(t, err)
		t.Log(err)
		assert.Nil(t, rep)
	})

	t.Run("all good", func(t *testing.T) {
		con := worklet.MockConsumer{}
		con.ShouldErr = false
		con.Message = pkg.Message{}
		rawS.consumerRepo.Put(context.Background(), job.Name, &con)
		defer rawS.consumerRepo.Delete(context.Background(), job.Name)

		req := &dispatcher.WorkerAckMsgsRequest{Name: job.Name, MsgIDs: []uint64{1}}
		rep, err := s.AckMsgs(context.Background(), req)
		assert.NoError(t, err)
		assert.NotNil(t, rep)
	})
}

func TestService_NackMsgsMsgs(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	job := &pkg.Job{
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
	mockController := &MockController{
		Job: job,
		Mu:  sync.RWMutex{},
	}
	consumerRepo := cache.NewConsumerRepo()
	producerRepo := cache.NewProducerRepo()
	jobRepo := cache.NewJobRepo()
	s := NewService(123, mockController, jobRepo, consumerRepo, producerRepo, "alpha:8113", time.Second, logger)
	rawS := s.(*service)

	t.Run("bad request", func(t *testing.T) {
		req := &dispatcher.WorkerNackMsgsRequest{}
		rep, err := s.NackMsgs(context.Background(), req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("context done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req := &dispatcher.WorkerNackMsgsRequest{}
		rep, err := s.NackMsgs(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("worker not found", func(t *testing.T) {
		req := &dispatcher.WorkerNackMsgsRequest{Name: job.Name, MsgIDs: []uint64{1}}
		rep, err := s.NackMsgs(context.Background(), req)
		assert.Error(t, err)
		assert.NotNil(t, err.Error(), "no consumer worklet found")
		assert.Nil(t, rep)
	})

	t.Run("consumer errs", func(t *testing.T) {
		con := worklet.MockConsumer{}
		con.ShouldErr = true
		rawS.consumerRepo.Put(context.Background(), job.Name, &con)
		defer rawS.consumerRepo.Delete(context.Background(), job.Name)

		req := &dispatcher.WorkerNackMsgsRequest{Name: job.Name, MsgIDs: []uint64{1}}
		rep, err := s.NackMsgs(context.Background(), req)
		assert.Error(t, err)
		t.Log(err)
		assert.Nil(t, rep)
	})

	t.Run("all good", func(t *testing.T) {
		con := worklet.MockConsumer{}
		con.ShouldErr = false
		con.Message = pkg.Message{}
		rawS.consumerRepo.Put(context.Background(), job.Name, &con)
		defer rawS.consumerRepo.Delete(context.Background(), job.Name)

		req := &dispatcher.WorkerNackMsgsRequest{Name: job.Name, MsgIDs: []uint64{1}}
		rep, err := s.NackMsgs(context.Background(), req)
		assert.NoError(t, err)
		assert.NotNil(t, rep)
	})
}

func TestService_PublishMsg(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	job := &pkg.Job{
		ID:          101,
		Name:        "alpha",
		Description: "beta",
		Type:        pkg.ProducerJob,
		State:       pkg.Stopped,
		QueueConfig: &pkg.QueueConfig{
			Type:   pkg.KafkaQueue,
			Config: pkg.KafkaConfig{ClusterAddress: []string{"alpha"}, Topic: "alpha"}},
		RetryConfig:       &pkg.RetryConfig{BackOffs: []uint{1}},
		MaxProcessingTime: 10,
		Version:           1,
	}
	mockController := &MockController{
		Job: job,
		Mu:  sync.RWMutex{},
	}
	consumerRepo := cache.NewConsumerRepo()
	producerRepo := cache.NewProducerRepo()
	jobRepo := cache.NewJobRepo()

	s := NewService(123, mockController, jobRepo, consumerRepo, producerRepo, "alpha:8113", time.Second, logger)
	rawS := s.(*service)

	t.Run("bad request", func(t *testing.T) {
		req := &dispatcher.WorkerPublishMsgsRequest{Name: "wow"}
		rep, err := s.PublishMsg(context.Background(), req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	t.Run("context done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req := &dispatcher.WorkerPublishMsgsRequest{}
		rep, err := s.PublishMsg(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, rep)
	})

	msgs := []*pkg.Message{&pkg.Message{ID: 1}}
	t.Run("worklet not found", func(t *testing.T) {
		req := &dispatcher.WorkerPublishMsgsRequest{Name: job.Name, Msgs: msgs}
		rep, err := s.PublishMsg(context.Background(), req)
		assert.Error(t, err)
		assert.NotNil(t, err.Error(), "no producer found")
		assert.Nil(t, rep)
	})

	t.Run("producer errs", func(t *testing.T) {
		p := worklet.MockProducer{}
		p.ShouldErr = true
		rawS.producerRepo.Put(context.Background(), job.Name, &p)
		defer rawS.producerRepo.Delete(context.Background(), job.Name)

		req := &dispatcher.WorkerPublishMsgsRequest{Name: job.Name, Msgs: msgs}
		rep, err := s.PublishMsg(context.Background(), req)
		assert.Error(t, err)
		t.Log(err)
		assert.Nil(t, rep)
	})

	t.Run("all good", func(t *testing.T) {
		p := worklet.MockProducer{}
		p.ShouldErr = false
		rawS.producerRepo.Put(context.Background(), job.Name, &p)
		defer rawS.producerRepo.Delete(context.Background(), job.Name)

		req := &dispatcher.WorkerPublishMsgsRequest{Name: job.Name, Msgs: msgs}
		rep, err := s.PublishMsg(context.Background(), req)
		assert.NoError(t, err)
		assert.NotNil(t, rep)
	})
}

func TestService_syncer(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	job := &pkg.Job{
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
	mockController := &MockController{
		Job: job,
		Mu:  sync.RWMutex{},
	}
	consumerRepo := cache.NewConsumerRepo()
	producerRepo := cache.NewProducerRepo()
	jobRepo := cache.NewJobRepo()

	t.Run("closing syncer", func(t *testing.T) {
		s := NewService(123, mockController, jobRepo, consumerRepo, producerRepo, "alpha:8113", time.Millisecond*10, logger)
		rawS := s.(*service)

		rawS.syncDone = make(chan struct{})
		go func() {
			time.Sleep(20 * time.Millisecond)
			close(rawS.syncDone)
		}()
		t1 := time.Now()
		rawS.syncer()
		t2 := time.Since(t1)
		assert.True(t, t2.Nanoseconds() < int64(100*1e6)) //100ms
	})

	t.Run("job status in sync errs", func(t *testing.T) {
		s := NewService(123, mockController, jobRepo, consumerRepo, producerRepo, "alpha:8113", time.Millisecond*10, logger)
		rawS := s.(*service)

		rawS.syncDone = make(chan struct{})
		go func() {
			time.Sleep(20 * time.Millisecond)
			close(rawS.syncDone)
		}()
		mockController.GossipErr = true
		rawS.syncer()
	})

	t.Run("job status in sync brings a job", func(t *testing.T) {
		s := NewService(123, mockController, jobRepo, consumerRepo, producerRepo, "alpha:8113", time.Millisecond*10, logger)
		rawS := s.(*service)

		rawS.syncDone = make(chan struct{})
		con := worklet.MockConsumer{}
		con.ShouldErr = false
		con.SyncSignal = make(chan struct{})
		rawS.consumerRepo.Put(context.Background(), job.Name, &con)
		defer rawS.consumerRepo.Delete(context.Background(), job.Name)

		go func() {
			select {
			case <-time.After(time.Second):
				t.Fatal("consumer worklet sync not called")
			case <-con.SyncSignal:
			}
			close(rawS.syncDone)
		}()
		mockController.GossipErr = false
		rawS.syncer()
	})
}
