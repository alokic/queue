package kafka

import (
	"encoding/json"
	"github.com/honestbee/gopkg/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestProducer_Publish(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})

	p := NewProducer("test", ProducerConfig{}, logger)
	b, _ := json.Marshal("alpha")

	//publish on uninited producer
	assert.Error(t, p.Publish([]json.RawMessage{b}))

	rawp := p.(*producer)
	producer := &MockProducer{WriteStream: make(chan struct{}, 1)}
	rawp.producer = producer
	assert.NoError(t, p.Publish([]json.RawMessage{b}))
	select {
	case <-producer.WriteStream:
	case <-time.After(time.Second):
		t.Fatal("timedout waiting for publish ack")
	}
}

func TestProducer_Close(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})

	p := NewProducer("test", ProducerConfig{}, logger)

	t.Run("close on uninited producer", func(t *testing.T) {
		err := p.Close()
		assert.Equal(t, ErrProducerInit, err.Error())
	})

	t.Run("close works", func(t *testing.T) {
		rawp := p.(*producer)
		producer := &MockProducer{WriteStream: make(chan struct{}, 1)}
		rawp.producer = producer
		assert.NoError(t, p.Close())
	})
}
