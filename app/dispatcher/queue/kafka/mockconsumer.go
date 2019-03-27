package kafka

import (
	"errors"
	"github.com/honestbee/gopkg/kafka"
	"time"
)

//MockConsumer struct.
type MockConsumer struct {
	Msgstream    chan kafka.Msg
	commitStream chan struct{}
}

//Setup to setup consumer.
func (mc *MockConsumer) Setup() error {
	if mc.commitStream == nil {
		mc.commitStream = make(chan struct{}, 1)
	}
	if mc.Msgstream == nil {
		mc.Msgstream = make(chan kafka.Msg, 10)
	}

	return nil
}

//Poll for msgs.
func (mc *MockConsumer) Poll(p time.Duration) ([]kafka.Msg, error) {
	select {
	case <-time.After(p):
		return nil, errors.New("timed out")
	case msg := <-mc.Msgstream:
		return []kafka.Msg{msg}, nil
	}
}

//Commit msgs.
func (mc *MockConsumer) Commit(h []interface{}) error {
	mc.commitStream <- struct{}{}
	return nil
}

//Close it.
func (mc *MockConsumer) Close() error {
	close(mc.Msgstream)
	close(mc.commitStream)
	return nil
}
