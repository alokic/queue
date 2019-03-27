// Package worker is the home for worker client service.
package worker

import (
	"context"

	"fmt"
	"github.com/alokic/queue/app/dispatcher"
	"github.com/alokic/queue/app/dispatcher/queue"
	"github.com/alokic/queue/app/dispatcher/queue/kafka"
	"github.com/alokic/queue/app/dispatcher/worklet"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"time"
)

// ControllerSvc is interface to controller service.
type ControllerSvc interface {
	Gossip(uint64) ([]*pkg.Job, error)
	Register(uint64, string) (*pkg.Job, error)
	Deregister(uint64, uint64) error
}

type service struct {
	nodeID         uint64
	consumerRepo   dispatcher.ConsumerRepository
	producerRepo   dispatcher.ProducerRepository
	jobsRepo       dispatcher.JobRepository
	controller     ControllerSvc
	nativeQueueAdd string
	syncDone       chan struct{}
	syncInterval   time.Duration
	logger         logger.Logger
}

// NewService is constructor of manager service.
func NewService(nodeID uint64, controller ControllerSvc, jobsRepo dispatcher.JobRepository, consumer dispatcher.ConsumerRepository, producer dispatcher.ProducerRepository, nqa string, syncInterval time.Duration, lg logger.Logger) dispatcher.WorkerService {
	logger := lg.ContextualLogger(map[string]interface{}{"context": "worker-service"})
	return &service{
		nodeID:         nodeID,
		controller:     controller,
		nativeQueueAdd: nqa,
		jobsRepo:       jobsRepo,
		consumerRepo:   consumer,
		producerRepo:   producer,
		syncInterval:   syncInterval,
		logger:         logger,
	}
}

func (s *service) Init() {
	s.syncDone = make(chan struct{})
	go s.syncer()
}

// SubscribeJob subscribes to received update about job from controller.
func (s *service) SubscribeJob(ctx context.Context, in *dispatcher.WorkerSubscribeJobRequest) (*dispatcher.WorkerSubscribeJobReply, error) {
	if err := in.Validate(); err != nil {
		return nil, pkg.ErrBadRequest{Err: err}
	}

	select {
	case <-ctx.Done():
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	job := s.jobsRepo.Get(ctx, in.Name)
	if job == nil {
		if err := s.subscribeJob(ctx, in.Name); err != nil {
			switch err.(type) {
			case pkg.ErrNotFound:
				return nil, err
			default:
				return nil, errors.Wrap(err, "error while subscribing job")
			}
		}
	} else {
		s.logger.Infof("job: %v already subscribed", in.Name)
	}

	reply := new(dispatcher.WorkerSubscribeJobReply)
	return reply, nil
}

func (s *service) subscribeJob(ctx context.Context, name string) error {
	job, err := s.controller.Register(s.nodeID, name)
	if err != nil {
		return err
	}

	switch job.Type {
	case pkg.ConsumerJob:
		worklet, err := s.startConsumer(job)
		if err != nil {
			return errors.Wrap(err, "error in starting consumer worklet")
		}
		s.consumerRepo.Put(ctx, name, worklet)
		s.jobsRepo.Put(ctx, name, job)
		return nil
	case pkg.ProducerJob:
		worklet, err := s.startProducer(job)
		if err != nil {
			return errors.Wrap(err, "error in starting producer worklet")
		}
		s.producerRepo.Put(ctx, name, worklet)
		s.jobsRepo.Put(ctx, name, job)
		return nil
	default:
		return errors.New(fmt.Sprintf("bad job type: %v", job.Type))
	}
}

// UnsubscribeJob unsubscribes job from controller.
func (s *service) UnsubscribeJob(ctx context.Context, in *dispatcher.WorkerUnsubscribeJobRequest) (*dispatcher.WorkerUnsubscribeJobReply, error) {
	if err := in.Validate(); err != nil {
		return nil, pkg.ErrBadRequest{Err: err}
	}

	select {
	case <-ctx.Done():
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	job := s.jobsRepo.Get(ctx, in.Name)
	if job != nil {
		if err := s.unsubscribeJob(ctx, job); err != nil {
			return nil, errors.Wrap(err, "error while unsubscribing job")
		}
	} else {
		s.logger.Infof("job: %v already unsubscribed", in.Name)
	}

	reply := new(dispatcher.WorkerUnsubscribeJobReply)
	return reply, nil
}

func (s *service) unsubscribeJob(ctx context.Context, job *pkg.Job) error {
	err := s.controller.Deregister(s.nodeID, job.ID)
	if err != nil {
		return errors.Wrap(err, "error while unregistering from controller")
	}

	switch job.Type {
	case pkg.ConsumerJob:
		con := s.consumerRepo.Get(ctx, job.Name)
		if con == nil {
			return errors.New("no consumer worklet found")
		}
		_ = s.stopConsumer(con) //ignore errors
		s.consumerRepo.Delete(ctx, job.Name)
		s.jobsRepo.Delete(ctx, job.Name)
		return nil
	case pkg.ProducerJob:
		p := s.producerRepo.Get(ctx, job.Name)
		if p == nil {
			return errors.New("no producer worklet found")
		}
		_ = s.stopProducer(p) //ignore errors
		s.producerRepo.Delete(ctx, job.Name)
		s.jobsRepo.Delete(ctx, job.Name)
		return nil
	default:
		return errors.New(fmt.Sprintf("bad job type: %v", job.Type))
	}
}

// GetMsgs pull messages from queue.
func (s *service) GetMsgs(ctx context.Context, in *dispatcher.WorkerGetMsgsRequest) (*dispatcher.WorkerGetMsgsReply, error) {
	if err := in.Validate(); err != nil {
		return nil, pkg.ErrBadRequest{Err: err}
	}

	select {
	case <-ctx.Done():
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	con := s.consumerRepo.Get(ctx, in.Name)
	if con == nil {
		return nil, pkg.ErrNotFound{Err: errors.New("no consumer found")}
	}
	msgs, err := con.GetMsgs(ctx, in.MaxWait)
	if err != nil {
		switch err.(type) {
		case pkg.ErrServiceUnavailable:
			return nil, err
		default:
			return nil, errors.Wrap(err, "error in getting messages")
		}
	}

	reply := new(dispatcher.WorkerGetMsgsReply)
	reply.Msgs = msgs
	return reply, nil
}

// GetRetryMsgs pull messages from retry queue.
func (s *service) GetRetryMsgs(ctx context.Context, in *dispatcher.WorkerGetRetryMsgsRequest) (*dispatcher.WorkerGetRetryMsgsReply, error) {
	if err := in.Validate(); err != nil {
		return nil, pkg.ErrBadRequest{Err: err}
	}

	select {
	case <-ctx.Done():
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	con := s.consumerRepo.Get(ctx, in.Name)
	if con == nil {
		return nil, pkg.ErrNotFound{Err: errors.New("no consumer found")}
	}

	msgs, err := con.GetRetryMsgs(ctx, in.RetryIndex, in.MaxWait)
	if err != nil {
		switch err.(type) {
		case pkg.ErrServiceUnavailable:
			return nil, err
		default:
			return nil, errors.Wrap(err, "error in getting messages")
		}
	}

	reply := new(dispatcher.WorkerGetRetryMsgsReply)
	reply.Msgs = msgs
	return reply, nil
}

// Get messages from DLQ.
func (s *service) GetDLQMsgs(ctx context.Context, in *dispatcher.WorkerGetDLQMsgsRequest) (*dispatcher.WorkerGetDLQMsgsReply, error) {
	if err := in.Validate(); err != nil {
		return nil, pkg.ErrBadRequest{Err: err}
	}

	select {
	case <-ctx.Done():
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	con := s.consumerRepo.Get(ctx, in.Name)
	if con == nil {
		return nil, pkg.ErrNotFound{Err: errors.New("no consumer found")}
	}

	msgs, err := con.GetDLQMsgs(ctx, in.MaxWait)
	if err != nil {
		switch err.(type) {
		case pkg.ErrServiceUnavailable:
			return nil, err
		default:
			return nil, errors.Wrap(err, "error in getting messages")
		}
	}

	reply := new(dispatcher.WorkerGetDLQMsgsReply)
	reply.Msgs = msgs
	return reply, nil
}

// AckMsgs acks messages in queue.
func (s *service) AckMsgs(ctx context.Context, in *dispatcher.WorkerAckMsgsRequest) (*dispatcher.WorkerAckMsgsReply, error) {
	if err := in.Validate(); err != nil {
		return nil, pkg.ErrBadRequest{Err: err}
	}

	select {
	case <-ctx.Done():
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	con := s.consumerRepo.Get(ctx, in.Name)
	if con == nil {
		return nil, pkg.ErrNotFound{Err: errors.New("no consumer found")}
	}

	failedIDs, badIDs, err := con.Ack(ctx, in.MsgIDs)

	if failedIDs == nil && badIDs == nil && err != nil {
		return nil, errors.Wrap(err, "error in acking messages")
	}

	reply := new(dispatcher.WorkerAckMsgsReply)
	reply.FailedIDs = failedIDs
	reply.BadIDs = badIDs
	if err != nil {
		reply.Error = err.Error()
	}

	return reply, nil
}

// NackMsgs nacks messages in queue.
func (s *service) NackMsgs(ctx context.Context, in *dispatcher.WorkerNackMsgsRequest) (*dispatcher.WorkerNackMsgsReply, error) {
	if err := in.Validate(); err != nil {
		return nil, pkg.ErrBadRequest{Err: err}
	}

	select {
	case <-ctx.Done():
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	con := s.consumerRepo.Get(ctx, in.Name)
	if con == nil {
		return nil, pkg.ErrNotFound{Err: errors.New("no consumer found")}
	}

	failedIDs, badIDs, err := con.Nack(ctx, in.MsgIDs)

	if failedIDs == nil && badIDs == nil && err != nil {
		return nil, errors.Wrap(err, "error in nacking messages")
	}

	reply := new(dispatcher.WorkerNackMsgsReply)
	reply.FailedIDs = failedIDs
	reply.BadIDs = badIDs
	if err != nil {
		reply.Error = err.Error()
	}

	return reply, nil
}

// Publish  messages in queue.
func (s *service) PublishMsg(ctx context.Context, in *dispatcher.WorkerPublishMsgsRequest) (*dispatcher.WorkerPublishMsgsReply, error) {
	if err := in.Validate(); err != nil {
		return nil, pkg.ErrBadRequest{Err: err}
	}

	select {
	case <-ctx.Done():
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	p := s.producerRepo.Get(ctx, in.Name)
	if p == nil {
		return nil, pkg.ErrNotFound{Err: errors.New("no producer found")}
	}

	if err := p.Publish(in.Msgs); err != nil {
		return nil, pkg.ErrInternalServerError{Err: err}
	}

	return &dispatcher.WorkerPublishMsgsReply{}, nil
}

func (s *service) Stop() {
	if s.syncDone != nil {
		close(s.syncDone)
	}
}

func (s *service) syncer() {
	s.logger.Info("starting syncer")

	t := time.NewTicker(s.syncInterval)
	for {
		select {
		case <-s.syncDone:
			s.logger.Info("stopping syncer")
			return
		case <-t.C:
			if jobs, err := s.controller.Gossip(s.nodeID); err != nil {
				s.logger.Warnf("error in syncing status from controller: %v", err)
			} else {
				//issue command to respective worklet
				for _, job := range jobs {
					if job != nil {
						switch job.Type {
						case pkg.ConsumerJob:
							con := s.consumerRepo.Get(context.Background(), job.Name)
							//controller can think worklet is registered but we might have failed to start it up.
							if con != nil {
								con.HandleSync(context.Background(), job)
							}
						case pkg.ProducerJob:
							p := s.producerRepo.Get(context.Background(), job.Name)
							//controller can think worklet is registered but we might have failed to start it up.
							if p != nil {
								p.HandleSync(context.Background(), job)
							}
						}
					}
				}
			}
		}
	}
}

func (s *service) startConsumer(j *pkg.Job) (dispatcher.Consumer, error) {
	switch j.QueueConfig.Type {
	case pkg.KafkaQueue:
		return s.startKafkaConsumer(j)
	default:
		return nil, errors.New(fmt.Sprintf("bad queue type: %v", j.QueueConfig.Type))
	}
}

func (s *service) startKafkaConsumer(j *pkg.Job) (dispatcher.Consumer, error) {
	kc := new(kafka.ConsumerConfig)
	if err := mapstructure.Decode(j.QueueConfig.Config, kc); err != nil {
		return nil, errors.New(fmt.Sprintf("bad kafka config in register: %v", j.QueueConfig.Config))
	}

	retryConsumers, retryProducers := getRetryPair(j, s.nativeQueueAdd, s.logger)
	dlqConsumer, dlqProducer := getDLQPair(j, s.nativeQueueAdd, s.logger)
	ackProducer := getAckMgr(j, s.nativeQueueAdd, s.logger)

	receiver := kafka.NewConsumer(j.Name, *kc, false, s.logger)

	con := worklet.NewConsumer(j, receiver, dlqConsumer, retryConsumers, s.logger)
	if err := con.Init(ackProducer, retryProducers, dlqProducer); err != nil {
		return nil, errors.Wrap(err, "error in initing consumer worklet")
	}

	return con, nil
}

func (s *service) stopConsumer(c dispatcher.Consumer) error {
	return c.Close()
}

func (s *service) startProducer(j *pkg.Job) (dispatcher.Producer, error) {
	switch j.QueueConfig.Type {
	case pkg.KafkaQueue:
		return s.startKafkaProducer(j)
	default:
		return nil, errors.New(fmt.Sprintf("bad queue type: %v", j.QueueConfig.Type))
	}
}

func (s *service) startKafkaProducer(j *pkg.Job) (dispatcher.Producer, error) {
	kc := new(kafka.ProducerConfig)
	if err := mapstructure.Decode(j.QueueConfig.Config, kc); err != nil {
		return nil, errors.New(fmt.Sprintf("bad producer kafka config in register: %v", j.QueueConfig.Config))
	}

	q := kafka.NewProducer(j.Name, *kc, s.logger)
	producer := worklet.NewProducer(j, q, s.logger)

	if err := producer.Init(); err != nil {
		return nil, errors.Wrap(err, "error in initing producer worklet")
	}

	return producer, nil
}

func (s *service) stopProducer(p dispatcher.Producer) error {
	return p.Close()
}

var getRetryPair = func(j *pkg.Job, nativeQueueAdd string, logger logger.Logger) ([]queue.Consumer, []queue.Producer) {
	retryProducers := []queue.Producer{}
	retryConsumers := []queue.Consumer{}

	for i := range j.RetryConfig.BackOffs {
		name := fmt.Sprintf("%v_retry_%v", j.Name, i)
		rqpc := kafka.ProducerConfig{ClusterAddress: []string{nativeQueueAdd}, Topic: name}
		retryProducers = append(retryProducers, kafka.NewProducer(name, rqpc, logger))

		rqcc := kafka.ConsumerConfig{ClusterAddress: []string{nativeQueueAdd}, Topic: name}
		retryConsumers = append(retryConsumers, kafka.NewConsumer(name, rqcc, false, logger))
	}

	return retryConsumers, retryProducers
}

var getDLQPair = func(j *pkg.Job, nativeQueueAdd string, logger logger.Logger) (queue.Consumer, queue.Producer) {
	dlqname := fmt.Sprintf("%v_dlq", j.Name)

	dlqkpc := kafka.ProducerConfig{ClusterAddress: []string{nativeQueueAdd}, Topic: dlqname}
	dlqProducer := kafka.NewProducer(dlqname, dlqkpc, logger)

	dlqkcc := kafka.ConsumerConfig{ClusterAddress: []string{nativeQueueAdd}, Topic: dlqname}
	dlqConsumer := kafka.NewConsumer(dlqname, dlqkcc, true, logger)

	return dlqConsumer, dlqProducer
}

var getAckMgr = func(j *pkg.Job, nativeQueueAdd string, logger logger.Logger) queue.Producer {
	ackName := fmt.Sprintf("%v_ack", j.Name)

	ackkpc := kafka.ProducerConfig{ClusterAddress: []string{nativeQueueAdd}, Topic: ackName}
	ackProducer := kafka.NewProducer(ackName, ackkpc, logger)

	return ackProducer
}
