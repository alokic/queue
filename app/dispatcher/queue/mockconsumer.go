package queue

import (
	"errors"
	"time"
)

//MockConsumer struct.
type MockConsumer struct {
	Msgstream        chan Msg
	AckStream        chan struct{}
	AckReturnStream  chan bool
	NackStream       chan struct{}
	NackReturnStream chan bool
}

//Init it.
func (mc *MockConsumer) Init() error {
	if mc.AckStream == nil {
		mc.AckStream = make(chan struct{})
	}
	if mc.NackStream == nil {
		mc.NackStream = make(chan struct{})
	}
	if mc.AckReturnStream == nil {
		mc.AckReturnStream = make(chan bool)
	}
	if mc.NackReturnStream == nil {
		mc.NackReturnStream = make(chan bool)
	}
	if mc.Msgstream == nil {
		mc.Msgstream = make(chan Msg, 10)
	}

	return nil
}

//Poll it.
func (mc *MockConsumer) Poll(p time.Duration) ([]Msg, error) {
	select {
	case <-time.After(p):
		return nil, errors.New("timed out")
	case msg := <-mc.Msgstream:
		return []Msg{msg}, nil
	}
}

//Ack it.
func (mc *MockConsumer) Ack(h interface{}) error {
	mc.AckStream <- struct{}{}
	if msg := <-mc.AckReturnStream; !msg {
		return errors.New("something happened")
	}
	return nil
}

//Nack it.
func (mc *MockConsumer) Nack(h interface{}) error {
	mc.NackStream <- struct{}{}
	if msg := <-mc.NackReturnStream; !msg {
		return errors.New("something happened")
	}
	return nil
}

//Close it.
func (mc *MockConsumer) Close() error {
	if mc.Msgstream != nil {
		close(mc.Msgstream)
	}
	if mc.AckStream != nil {
		close(mc.AckStream)
	}
	if mc.NackStream != nil {
		close(mc.NackStream)
	}
	if mc.AckReturnStream != nil {
		close(mc.AckReturnStream)
	}
	if mc.NackReturnStream != nil {
		close(mc.NackReturnStream)
	}
	return nil
}
