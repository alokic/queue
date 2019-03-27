// Package worklet defines worklets for one job.
package worklet

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/alokic/queue/app/ackmgr"
	"github.com/alokic/queue/app/dispatcher"
	"github.com/alokic/queue/app/dispatcher/queue"
	"github.com/alokic/queue/app/fom"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/pkg/errors"
)

type (
	consumerCapability uint

	queueMeta struct {
		q   queue.Consumer
		msg queue.Msg
	}

	//Consumer struct.
	Consumer struct {
		job                *pkg.Job
		failedInit         bool
		mainReceiver       queue.Consumer
		retryReceivers     []queue.Consumer
		dlqReceiver        queue.Consumer
		capabilities       consumerCapability
		mainMessagesCache  []pkg.Message
		dlqMessagesCache   []pkg.DLQMessage
		retryMessagesCache map[uint][]pkg.Message
		messageToQueue     map[uint64]queueMeta //ack/nack handles
		mu                 sync.Mutex
		fom                *fom.FOM
		ackmgr             *ackmgr.AckMgr
		logger             logger.Logger
	}
)

const (
	poll consumerCapability = 1 << 0
	ack  consumerCapability = 1 << 1
	nack consumerCapability = 1 << 2

	idAttempts = 5
)

var (
	maxUncommitedJobs = 1000
)

//NewConsumer returns new consumer job.
func NewConsumer(j *pkg.Job, mc, dlqc queue.Consumer, rc []queue.Consumer, lg logger.Logger) dispatcher.Consumer {
	logger := lg.ContextualLogger(map[string]interface{}{"name": j.Name, "context": "consumer-job"})
	cap := getConsumerCapability(j.State)

	c := Consumer{
		job:            j,
		failedInit:     true,
		mainReceiver:   mc,
		retryReceivers: rc,
		dlqReceiver:    dlqc,

		mainMessagesCache:  []pkg.Message{},
		dlqMessagesCache:   []pkg.DLQMessage{},
		retryMessagesCache: make(map[uint][]pkg.Message),
		messageToQueue:     make(map[uint64]queueMeta),
		capabilities:       cap,
		mu:                 sync.Mutex{},
		logger:             logger,
	}
	return &c
}

//Init to be called first.
func (c *Consumer) Init(ackProducer queue.Producer, retryProducers []queue.Producer, dlqProducer queue.Producer) error {
	defer func() {
		if r := recover(); r != nil || c.failedInit {
			c.Close()
		}
	}()

	c.logger.Infof("initing with capabilities: %v, version: %v", c.capabilities, c.job.Version)

	c.ackmgr = ackmgr.New(c.job.Name, ackProducer, c.logger)
	if err := c.ackmgr.Init(); err != nil {
		return errors.Wrap(err, "error while initing AckMgr")
	}

	c.fom = fom.New(c.job.Name, c.job.RetryConfig, retryProducers, dlqProducer, c.logger)
	c.logger.Infof("FOM config: retryProducers: %v, dlqProducers: %v", retryProducers, dlqProducer)
	if err := c.fom.Init(); err != nil {
		return errors.Wrap(err, "error while initing FOM")
	}

	if err := c.mainReceiver.Init(); err != nil {
		return errors.Wrap(err, "error while initing main consumer")
	}

	for i, r := range c.retryReceivers {
		if err := r.Init(); err != nil {
			return errors.Wrap(err, fmt.Sprintf("error while initing retry consumer: %v", i))
		}
	}

	if err := c.dlqReceiver.Init(); err != nil {
		return errors.Wrap(err, "error while initing DLQ consumer")
	}

	c.failedInit = false
	return nil
}

//GetMsgs gets message from main topic.
func (c *Consumer) GetMsgs(ctx context.Context, maxWait uint) ([]pkg.Message, error) {
	select {
	case <-ctx.Done():
		c.logger.Warn("context done before starting GetMsgs")
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	if c.failedInit {
		c.logger.Warnf("Attempt to GetMsgs on failed consumer")
		return nil, errors.New("Attempt to GetMsgs on failed consumer")
	}

	if c.capabilities&poll == 0 {
		c.logger.Info("ignoring poll request as we are not capable of serving it: %v", c.capabilities)
		return nil, pkg.ErrServiceUnavailable{Err: errors.New("not capable of serving poll request")}
	}

	if len(c.messageToQueue) > maxUncommitedJobs {
		c.logger.Warnf("not serving poll request as many uncommited messages.")
		return nil, pkg.ErrServiceUnavailable{Err: errors.New("many unacked/unnacked messages.please ack/nack them first")}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.mainMessagesCache) > 0 {
		defer func() { c.mainMessagesCache = []pkg.Message{} }() //empty cache
		return c.mainMessagesCache, nil
	}

	messages, err := c.poll(ctx, c.mainReceiver, maxWait)
	if err != nil {
		return nil, errors.Wrap(err, "error in getting messages")
	}

	select {
	case <-ctx.Done():
		c.logger.Warn("context done after getting messages. caching messages")
		c.mainMessagesCache = messages
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	return messages, nil
}

//GetRetryMsgs gets retry msgs.
func (c *Consumer) GetRetryMsgs(ctx context.Context, retryIndex, maxWait uint) ([]pkg.Message, error) {
	select {
	case <-ctx.Done():
		c.logger.Warn("context done before starting GetRetryMsgs")
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	if c.failedInit {
		c.logger.Warnf("Attempt to GetRetryMsgs on failed consumer")
		return nil, errors.New("Attempt to GetRetryMsgs on failed consumer")
	}

	if retryIndex >= uint(len(c.retryReceivers)) {
		return nil, errors.New(fmt.Sprintf("bad retryIndex: %v", retryIndex))
	}

	if c.capabilities&poll == 0 {
		c.logger.Info("ignoring poll request as we are not capable of serving it: %v", c.capabilities)
		return nil, pkg.ErrServiceUnavailable{Err: errors.New("not capable of serving poll request")}
	}

	if len(c.messageToQueue) > maxUncommitedJobs {
		c.logger.Warnf("not serving poll request as many uncommited messages.")
		return nil, pkg.ErrServiceUnavailable{Err: errors.New("many unacked/unnacked messages.please ack/nack them first")}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.retryMessagesCache[retryIndex]) > 0 {
		defer func() { c.retryMessagesCache[retryIndex] = []pkg.Message{} }() //empty cache
		return c.retryMessagesCache[retryIndex], nil
	}

	messages, err := c.poll(ctx, c.retryReceivers[retryIndex], maxWait)
	if err != nil {
		return nil, errors.Wrap(err, "error in getting messages")
	}
	select {
	case <-ctx.Done():
		c.logger.Warn("context done after getting messages. caching messages")
		c.retryMessagesCache[retryIndex] = messages
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}
	return messages, nil
}

func (c *Consumer) poll(ctx context.Context, q queue.Consumer, maxWait uint) ([]pkg.Message, error) {
	msgs, err := q.Poll(time.Duration(maxWait) * time.Millisecond)
	if err != nil {
		return nil, err
	}

	messages := []pkg.Message{}
	for _, msg := range msgs {
		j, err := pkg.DecodeMessage(msg.Data)
		if err != nil {
			c.logger.Warnf("Unable to decode Job from Msg, job: %v, err: %v\n", string(msg.Data), err)
			q.Nack(msg.Meta()) //movie commit pointer forward
			c.fom.ToDLQ(msg.Data, "unable_to_decode_Job")
		} else {
			ensureID(j)
			j.Attempt++
			messages = append(messages, *j)
			msg.Data, _ = j.Encode()                           //update msg with job
			c.messageToQueue[j.ID] = queueMeta{q: q, msg: msg} //handle for ack/nack
		}
	}

	return messages, nil //might be empty
}

//GetDLQMsgs gets dlq msgs. we don't check capabilities here as they dont need to be acked manually.
func (c *Consumer) GetDLQMsgs(ctx context.Context, maxWait uint) ([]pkg.DLQMessage, error) {
	select {
	case <-ctx.Done():
		c.logger.Warn("context done before starting GetDLQMsgs")
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	if c.failedInit {
		c.logger.Warnf("Attempt to GetDLQMsgs on failed consumer")
		return nil, errors.New("Attempt to GetDLQMsgs on failed consumer")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.dlqMessagesCache) > 0 {
		defer func() { c.dlqMessagesCache = []pkg.DLQMessage{} }() //empty cache
		return c.dlqMessagesCache, nil
	}

	messages, err := c.pollDLQ(ctx, c.dlqReceiver, maxWait)
	if err != nil {
		return nil, errors.Wrap(err, "error in getting messages")
	}
	select {
	case <-ctx.Done():
		c.logger.Warn("context done after getting messages. caching messages")
		c.dlqMessagesCache = messages
		return nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}
	return messages, nil
}

func (c *Consumer) pollDLQ(ctx context.Context, q queue.Consumer, maxWait uint) ([]pkg.DLQMessage, error) {
	msgs, err := q.Poll(time.Duration(maxWait) * time.Millisecond)
	if err != nil {
		return nil, err
	}

	messages := []pkg.DLQMessage{}

	for _, msg := range msgs {
		j, err := pkg.DecodeDLQMessage(msg.Data)
		if err != nil { //no need to nack as it runs in autocommit mode
			c.logger.Warnf("Unable to decode DLQMessage from Msg, msg: %v, err: %v\n", msg.Data, err)
		} else {
			messages = append(messages, *j)
		}
	}

	return messages, nil //might be empty
}

//Ack acks a message for main/retry queue.
func (c *Consumer) Ack(ctx context.Context, ids []uint64) ([]uint64, []uint64, error) {
	select {
	case <-ctx.Done():
		c.logger.Warn("context done before starting ack")
		return nil, nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	if c.failedInit {
		c.logger.Warnf("Attempt to Ack on failed consumer")
		return nil, nil, errors.New("Attempt to Ack on failed consumer")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.capabilities&ack == 0 {
		c.logger.Warnf("ignoring ack request as we are not capable of serving it: %v", c.capabilities)
		return nil, nil, errors.New("not capable of serving ack request")
	}

	return c.ack(ctx, ids)
}

func (c *Consumer) ack(ctx context.Context, ids []uint64) ([]uint64, []uint64, error) {
	//Note: we dont sort as ids might have been generated by enqueuer
	var err error
	badIds := []uint64{}
	failedIDIndex := 0

LOOP:
	for i, id := range ids {
		select {
		case <-ctx.Done():
			//TODO add metrics for these
			c.logger.Infof("context done before acking for: %v", ids[failedIDIndex:])
			err = errors.New("timedout")
			break LOOP
		default:
			if qMeta, ok := c.messageToQueue[id]; !ok {
				badIds = append(badIds, id)
				c.logger.Warnf("job id: %v for ack not found. Maybe already acked", id)
			} else {
				if err = qMeta.q.Ack(qMeta.msg.Meta()); err != nil {
					break LOOP
				}
				//do a best effort push to ackmgr, if failed we just log it and not fail the request.
				msg, _ := pkg.DecodeMessage(qMeta.msg.Data) //we wont expect err here
				if err := c.ackmgr.Push(pkg.AckMessage{ID: msg.ID, ExternalID: msg.ExternalID, Name: msg.Name, Attempt: msg.Attempt}); err != nil {
					c.logger.Warnf("error in pushing to ack queue: %v. Ignoring.", err)
				}
				delete(c.messageToQueue, id) //should we delete it after T minutes?
			}
			failedIDIndex = i + 1
		}
	}

	return ids[failedIDIndex:], badIds, err
}

//Nack nacks a message for main/retry queue.
func (c *Consumer) Nack(ctx context.Context, ids []uint64) ([]uint64, []uint64, error) {
	select {
	case <-ctx.Done():
		c.logger.Warn("context done before starting nack")
		return nil, nil, pkg.ErrTimedout{Err: errors.New("timedout")}
	default:
	}

	if c.failedInit {
		c.logger.Warnf("Attempt to Nack on failed consumer")
		return nil, nil, errors.New("Attempt to Nack on failed consumer")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.capabilities&nack == 0 {
		c.logger.Warnf("ignoring nack request as we are not capable of serving it: %v", c.capabilities)
		return nil, nil, errors.New("not capable of serving nack request")
	}

	return c.nack(ctx, ids)
}

func (c *Consumer) nack(ctx context.Context, ids []uint64) ([]uint64, []uint64, error) {
	var err error
	badIds := []uint64{}
	failedIDIndex := 0

LOOP:
	for i, id := range ids {
		select {
		case <-ctx.Done():
			//TODO add metrics for these
			c.logger.Infof("context done before acking for: %v", ids[failedIDIndex:])
			err = errors.New("timedout")
			break LOOP
		default:
			if qMeta, ok := c.messageToQueue[id]; !ok {
				badIds = append(badIds, id)
				c.logger.Warnf("job id: %v for ack not found. Maybe already acked", id)
			} else {
				msg, _ := pkg.DecodeMessage(qMeta.msg.Data) //we wont expect err here
				if err = c.fom.Push(*msg); err != nil {
					break LOOP
				}

				if err = qMeta.q.Nack(qMeta.msg.Meta()); err != nil {
					break LOOP
				}
				delete(c.messageToQueue, id) //should we delete it after T minutes?
			}
			failedIDIndex = i + 1
		}
	}

	return ids[failedIDIndex:], badIds, err
}

//HandleSync handles job updates from controller.
func (c *Consumer) HandleSync(ctx context.Context, new *pkg.Job) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cap := getConsumerCapability(new.State)

	if cap != c.capabilities {
		c.logger.Infof("consumer capability change: %v -> %v", c.capabilities, cap)
		c.capabilities = cap
	}
}

//Close stops consumer.
func (c *Consumer) Close() error {
	c.logger.Info("stopping")
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.messageToQueue) > 0 {
		c.logger.Warnf("Stopping with %v unacked messages", len(c.messageToQueue))
	}

	if c.mainReceiver != nil {
		c.mainReceiver.Close()
	}

	for _, r := range c.retryReceivers {
		if r != nil {
			r.Close()
		}
	}

	if c.fom != nil {
		c.fom.Close()
	}

	return nil
}

var getConsumerCapability = func(c pkg.JobState) consumerCapability {
	var cap consumerCapability

	switch c {
	case pkg.Active, pkg.Deprecated:
		cap = poll | ack | nack
	case pkg.Stopped, pkg.Archived:
		cap = ack | nack
	default:
		cap = poll | ack | nack
	}
	return cap
}

var ensureID = func(j *pkg.Message) {
	if j.Attempt == 0 {
		j.ID = pkg.MustGid(idAttempts)
	}
}
