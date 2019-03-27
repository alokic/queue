package kafka

import (
	"github.com/alokic/queue/app/dispatcher/queue"
	"github.com/honestbee/gopkg/kafka"
	logger2 "github.com/honestbee/gopkg/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func compareSlices(a, b []queue.Msg) bool {
	if a == nil && b == nil {
		return true
	}

	if a == nil || b == nil {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i, e := range a {
		if string(b[i].Data) != string(e.Data) {
			return false
		}
		if b[i].Meta() == nil {
			if e.Meta() != nil {
				return false
			}
		} else {
			if e.Meta() == nil {
				return false
			}
			if b[i].Meta().(kafka.TopicPartition) != e.Meta().(kafka.TopicPartition) {
				return false
			}
		}
	}
	return true
}

func TestConsumer_Poll(t *testing.T) {
	logger := logger2.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	con := NewConsumer("test", ConsumerConfig{}, true, logger)

	//poll on uninited consumer
	_, err := con.Poll(100)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), ErrConsumerInit.Error())

	rawconsumer := con.(*consumer)
	consumer := &MockConsumer{}
	consumer.Setup()
	rawconsumer.consumer = consumer

	//poll should timeout
	_, err = con.Poll(100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")

	//poll returns messages
	consumer.Msgstream <- kafka.Msg{}
	msgs, err := con.Poll(2000)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(msgs))
}

func TestConsumer_Nack(t *testing.T) {
	logger := logger2.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	con := NewConsumer("test", ConsumerConfig{}, true, logger)

	//nack on uninited consumer
	err := con.Nack("garbage")
	assert.Error(t, err)
	assert.Equal(t, err.Error(), ErrConsumerInit.Error())

	rawconsumer := con.(*consumer)
	consumer := &MockConsumer{}
	consumer.Setup()
	rawconsumer.consumer = consumer

	//nack works
	err = con.Nack("garbage")
	assert.NoError(t, err)

	select {
	case <-consumer.commitStream:
	case <-time.After(time.Second):
		t.Fatal("didnt get commit ack")
	}
}

func TestConsumer_Ack(t *testing.T) {
	logger := logger2.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	con := NewConsumer("test", ConsumerConfig{}, true, logger)

	//ack on uninited consumer
	err := con.Ack("garbage")
	assert.Error(t, err)
	assert.Equal(t, err.Error(), ErrConsumerInit.Error())

	rawconsumer := con.(*consumer)
	consumer := &MockConsumer{}
	consumer.Setup()
	rawconsumer.consumer = consumer

	//ack works
	err = con.Ack("garbage")
	assert.NoError(t, err)

	select {
	case <-consumer.commitStream:
	case <-time.After(time.Second):
		t.Fatal("didnt get commit ack")
	}
}

func TestConsumer_toMsg(t *testing.T) {
	tests := map[string]struct {
		input  []kafka.Msg
		output []queue.Msg
	}{
		"no msgs": {
			input:  []kafka.Msg{},
			output: []queue.Msg{},
		},
		"messages": {
			input:  []kafka.Msg{kafka.Msg{Data: []byte("aa")}},
			output: []queue.Msg{queue.Msg{Data: []byte("aa")}},
		},
	}

	for name, test := range tests {
		t.Log("Running: ", name)
		out := toMsg(test.input)
		assert.True(t, compareSlices(out, test.output))
	}
}

func TestConsumer_Close(t *testing.T) {
	logger := logger2.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	con := NewConsumer("test", ConsumerConfig{}, true, logger)

	t.Run("close on uninited consumer", func(t *testing.T) {
		err := con.Close()
		assert.Error(t, err)
		assert.Equal(t, err.Error(), ErrConsumerInit.Error())
	})

	t.Run("close works", func(t *testing.T) {
		rawconsumer := con.(*consumer)
		consumer := &MockConsumer{}
		consumer.Setup()
		rawconsumer.consumer = consumer

		err := con.Close()
		assert.NoError(t, err)
	})
}
