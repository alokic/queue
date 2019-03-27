package worklet

import (
	"context"
	"github.com/alokic/queue/pkg"
	"github.com/pkg/errors"
)

//MockProducer struct.
type MockProducer struct {
	ShouldErr  bool
	Message    pkg.Message
	SyncSignal chan struct{}
}

//Init initialises consumer.
func (mp *MockProducer) Init() error {
	if mp.ShouldErr {
		return errors.New("failed")
	}
	mp.SyncSignal = make(chan struct{})
	return nil
}

//Publish for getting msgs.
func (mp *MockProducer) Publish([]*pkg.Message) error {
	if mp.ShouldErr {
		return errors.New("failed")
	}
	return nil
}

//HandleSync for handling sync.
func (mp *MockProducer) HandleSync(context.Context, *pkg.Job) {
	mp.SyncSignal <- struct{}{}
}

//Close for closing.
func (mp *MockProducer) Close() error {
	if mp.SyncSignal != nil {
		close(mp.SyncSignal)
	}
	return nil
}
