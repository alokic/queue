package ackmgr

import (
	"encoding/json"
	"github.com/pkg/errors"

	"github.com/alokic/queue/app/dispatcher/queue"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
)

type (
	//AckMgr is ackmanager struct.
	AckMgr struct {
		name       string
		failedInit bool
		queue      queue.Producer
		logger     logger.Logger
	}
)

//New is contructutor for FOM.
func New(name string, queue queue.Producer, lg logger.Logger) *AckMgr {
	logger := lg.ContextualLogger(map[string]interface{}{"context": "ackmgr", "name": name})
	return &AckMgr{name: name, failedInit: true, queue: queue, logger: logger}
}

//Init Ackmgr.
func (am *AckMgr) Init() error {
	defer func() {
		if r := recover(); r != nil || am.failedInit {
			am.Close()
		}
	}()

	am.logger.Info("AckMgr Producer: %v", am.queue)
	if err := am.queue.Init(); err != nil {
		return errors.Wrap(err, "error in initing AckMgr producer")
	}
	am.failedInit = false
	return nil
}

//Push message to a ackmgr queue.
func (am *AckMgr) Push(msg pkg.AckMessage) error {
	if am.failedInit {
		am.logger.Warn("Attempt to push on failed/uninited AckMgr")
		return errors.New("Attempt to push on failed/uninited AckMgr")
	}

	b, err := json.Marshal(&msg)
	if err != nil {
		return errors.Wrapf(err, "unable to encode to AckMessage: %v", msg)
	}

	am.logger.Infof("pushing job: %v to ack queue", msg.ID)
	return am.queue.Publish([]json.RawMessage{b})
}

//Close is idempotent.
func (am *AckMgr) Close() error {
	if am.queue != nil {
		return am.queue.Close()
	}

	return nil
}
