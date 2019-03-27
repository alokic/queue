package fom

import (
	"encoding/json"
	"github.com/pkg/errors"

	"github.com/alokic/queue/app/dispatcher/queue"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
)

type (

	//FOM is failovermanager struct.
	FOM struct {
		name        string
		failedInit  bool
		rc          *pkg.RetryConfig
		retryQueues []queue.Producer
		dlq         queue.Producer
		logger      logger.Logger
	}
)

//New is contructutor for FOM.
func New(name string, rc *pkg.RetryConfig, retryQueues []queue.Producer, dlq queue.Producer, lg logger.Logger) *FOM {
	logger := lg.ContextualLogger(map[string]interface{}{"context": "fom", "name": name})
	return &FOM{name: name, failedInit: true, rc: rc, retryQueues: retryQueues, dlq: dlq, logger: logger}
}

//Init FOM.
func (f *FOM) Init() error {
	defer func() {
		if r := recover(); r != nil || f.failedInit {
			f.Close()
		}
	}()

	for _, rp := range f.retryQueues {
		if err := rp.Init(); err != nil {
			return errors.Wrap(err, "error in initing retry producer")
		}
	}

	f.logger.Info("DLQ Producer: %v", f.dlq)
	if err := f.dlq.Init(); err != nil {
		return errors.Wrap(err, "error in initing DLQ producer")
	}
	f.failedInit = false
	return nil
}

//Push message to a failover queue.FOM pushes directly to retry queue.
func (f *FOM) Push(job pkg.Message) error {
	if f.failedInit {
		f.logger.Warn("Attempt to push on failed/uninited FOM")
		return errors.New("Attempt to push on failed/uninited FOM")
	}

	f.logger.Infof("checking retry: %v (%v/%v)", job.ID, job.Attempt-1, job.ErrorPolicy.RepeatCount)

	if job.ErrorPolicy.RepeatCount == -1 || job.Attempt-1 < job.ErrorPolicy.RepeatCount {
		var qidx int
		if job.ErrorPolicy.RepeatQueue >= 0 {
			qidx = min(job.ErrorPolicy.RepeatQueue, len(f.retryQueues)-1)
		} else {
			qidx = min(job.Attempt-1, len(f.retryQueues)-1)
		}
		f.logger.Infof("sending %v to Retry queue: %v", job.ID, qidx)
		b, err := job.Encode()
		if err != nil {
			return errors.Wrap(err, "error while marshalling Job for retry/dlq")
		}

		return f.retryQueues[qidx].Publish([]json.RawMessage{b})
	}
	f.logger.Infof("sending to DLQ: %v", job.ID)

	b, err := job.Encode()
	if err != nil {
		return errors.Wrap(err, "error while marshalling Job for retry/dlq")
	}

	if err := f.ToDLQ(b, "max_retry"); err != nil {
		return errors.Wrap(err, "error while pushing to DLQ")
	}
	return nil
}

//ToDLQ to push to DLQ.
func (f *FOM) ToDLQ(msg []byte, reason string) error {
	if f.failedInit {
		f.logger.Warn("Attempt to push to DLQ on failed/uninited FOM")
		return errors.New("Attempt to push to DLQ on failed/uninited FOM")
	}

	dlqMsg := pkg.DLQMessage{RawJob: msg, Reason: reason}
	b, err := json.Marshal(&dlqMsg)
	if err != nil {
		return errors.Wrap(err, "unable to encode to DLQMessage")
	}
	return f.dlq.Publish([]json.RawMessage{b})
}

//Close is idempotent.
func (f *FOM) Close() error {
	for _, rq := range f.retryQueues {
		if rq != nil {
			rq.Close()
		}
	}

	if f.dlq != nil {
		f.dlq.Close()
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
