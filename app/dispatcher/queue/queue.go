// Package queue is top level package for all queues.
package queue

import (
	"encoding/json"
	"time"
)

// Consumer is the interface for all queue consumers.
type Consumer interface {
	Init() error
	Poll(time.Duration) ([]Msg, error)
	Ack(interface{}) error
	Nack(interface{}) error
	Close() error
}

// Producer is the interface for all queue producers.
type Producer interface {
	Init() error
	Publish([]json.RawMessage) error
	Close() error
}
