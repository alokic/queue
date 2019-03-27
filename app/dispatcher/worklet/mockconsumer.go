package worklet

import (
	"context"
	"github.com/alokic/queue/app/dispatcher/queue"
	"github.com/alokic/queue/pkg"
	"github.com/pkg/errors"
)

//MockConsumer struct.
type MockConsumer struct {
	ShouldErr  bool
	Message    pkg.Message
	DLQMessage pkg.DLQMessage
	FailedIDs  []uint64
	BadIDs     []uint64
	SyncSignal chan struct{}
}

//Init initialises consumer.
func (mc *MockConsumer) Init(queue.Producer, []queue.Producer, queue.Producer) error {
	if mc.ShouldErr {
		return errors.New("failed")
	}
	mc.SyncSignal = make(chan struct{})
	return nil
}

//GetMsgs for getting msgs.
func (mc *MockConsumer) GetMsgs(context.Context, uint) ([]pkg.Message, error) {
	if mc.ShouldErr {
		return nil, errors.New("failed")
	}
	return []pkg.Message{mc.Message}, nil
}

//GetRetryMsgs for getting retry msgs.
func (mc *MockConsumer) GetRetryMsgs(context.Context, uint, uint) ([]pkg.Message, error) {
	if mc.ShouldErr {
		return nil, errors.New("failed")
	}
	return []pkg.Message{mc.Message}, nil
}

//GetDLQMsgs for dlq msgs.
func (mc *MockConsumer) GetDLQMsgs(context.Context, uint) ([]pkg.DLQMessage, error) {
	if mc.ShouldErr {
		return nil, errors.New("failed")
	}
	return []pkg.DLQMessage{mc.DLQMessage}, nil
}

//Ack for acking msgs.
func (mc *MockConsumer) Ack(context.Context, []uint64) ([]uint64, []uint64, error) {
	if mc.ShouldErr {
		return nil, nil, errors.New("failed")
	}
	return mc.FailedIDs, mc.BadIDs, nil
}

//Nack for nacking msgs.
func (mc *MockConsumer) Nack(context.Context, []uint64) ([]uint64, []uint64, error) {
	if mc.ShouldErr {
		return nil, nil, errors.New("failed")
	}
	return mc.FailedIDs, mc.BadIDs, nil
}

//HandleSync for handling sync.
func (mc *MockConsumer) HandleSync(context.Context, *pkg.Job) {
	mc.SyncSignal <- struct{}{}
}

//Close for closing.
func (mc *MockConsumer) Close() error {
	if mc.ShouldErr {
		return errors.New("failed")
	}
	if mc.SyncSignal != nil {
		close(mc.SyncSignal)
	}
	return nil
}
