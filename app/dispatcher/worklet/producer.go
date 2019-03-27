package worklet

import (
	"context"
	"encoding/json"
	"github.com/alokic/queue/app/dispatcher"
	"github.com/alokic/queue/app/dispatcher/queue"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/pkg/errors"
	"sync"
)

//ProducerKafkaConfig is kafka config for kafka consumer queue.
type (
	producerCapability uint

	producer struct {
		job          *pkg.Job
		failedInit   bool
		queue        queue.Producer
		capabilities producerCapability
		mu           sync.Mutex
		logger       logger.Logger
	}
)

const (
	publish producerCapability = 1 << 0
)

//NewProducer constructor.
func NewProducer(j *pkg.Job, q queue.Producer, lg logger.Logger) dispatcher.Producer {
	logger := lg.ContextualLogger(map[string]interface{}{"name": j.Name, "context": "producer-worklet"})
	cap := getProducerCapability(j.State)

	return &producer{
		job:          j,
		failedInit:   true,
		queue:        q,
		capabilities: cap,
		mu:           sync.Mutex{},
		logger:       logger,
	}
}

//Init to be called first.
func (p *producer) Init() error {
	defer func() {
		if r := recover(); r != nil || p.failedInit {
			p.Close()
		}
	}()

	p.logger.Infof("initing with capabilities: %v, version: %v", p.capabilities, p.job.Version)

	if err := p.queue.Init(); err != nil {
		return errors.Wrap(err, "error in initing producer queue")
	}

	p.failedInit = false
	return nil
}

//Publish message.
func (p *producer) Publish(msgs []*pkg.Message) error {
	if p.failedInit {
		return errors.New("attempt to publish on failed producer")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.capabilities&publish == 0 {
		p.logger.Warnf("ignoring publish request as we are not capable of serving it: %v", p.capabilities)
		return errors.New("not capable of serving publish request")
	}

	msgsRaw := []json.RawMessage{}
	for _, m := range msgs {
		b, err := m.Encode()
		if err != nil {
			return errors.Wrap(err, "bad message")
		}
		msgsRaw = append(msgsRaw, b)
	}
	return p.queue.Publish(msgsRaw)
}

//HandleSync handles job updates from controller.
func (p *producer) HandleSync(ctx context.Context, new *pkg.Job) {
	p.mu.Lock()
	defer p.mu.Unlock()

	cap := getProducerCapability(new.State)

	if cap != p.capabilities {
		p.logger.Infof("producer capability change: %v -> %v", p.capabilities, cap)
		p.capabilities = cap
	}
}

//Close stops consumer.
func (p *producer) Close() error {
	p.logger.Info("stopping")
	return p.queue.Close()
}

var getProducerCapability = func(c pkg.JobState) producerCapability {
	var cap producerCapability

	switch c {
	case pkg.Active, pkg.Deprecated:
		cap = publish
	case pkg.Stopped, pkg.Archived:
		cap = 0
	default:
		cap = publish
	}
	return cap
}
