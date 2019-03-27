package queue

import (
	"encoding/json"
	"errors"
	"fmt"
)

//MockProducer is dummy producer for test.
type MockProducer struct {
	FailAction    bool
	FailInit      bool
	PublishStream chan struct{}
}

//Init method.
func (p *MockProducer) Init() error {
	fmt.Println("init in mockproducer")
	if p.FailInit {
		return errors.New("something happenned")
	}
	p.PublishStream = make(chan struct{}, 1000)
	return nil
}

//Publish method.
func (p *MockProducer) Publish(d []json.RawMessage) error {
	if p.FailAction {
		return errors.New("failed")
	}
	p.PublishStream <- struct{}{}
	return nil
}

//Close method.
func (p *MockProducer) Close() error {
	if p.PublishStream != nil {
		close(p.PublishStream)
	}
	return nil
}
