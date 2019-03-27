// Package kafka has implementation for kafka producer/consumer.
package kafka

import (
	"time"

	"github.com/pkg/errors"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/alokic/queue/app/dispatcher/queue"
	confluent "github.com/honestbee/gopkg/kafka"
	"github.com/honestbee/gopkg/logger"
)

// ConsumerConfig is kafka config for kafka consumer queue.
type ConsumerConfig struct {
	ClusterAddress []string               `json:"cluster_address" mapstructure:"cluster_address" validate:"required,min=1"`
	Topic          string                 `json:"topic" mapstructure:"topic" validate:"required"`
	Config         map[string]interface{} `json:"config" mapstructure:"config"`
}

type consumer struct {
	name       string
	consumer   confluent.KafkaConsumer
	autocommit bool
	config     ConsumerConfig
	logger     logger.Logger
}

var (
	// ErrConsumerInit means consumenr is not initialized.
	ErrConsumerInit = errors.New("failed to initialize kafka consumer")

	// ErrConsumerAck means we acking on nil consumer.
	ErrConsumerAck = errors.New("ack message failed as kafka consumer is not initialized")

	// ErrConsumerNack means we nacking on nil consumer.
	ErrConsumerNack = errors.New("nack message failed as kafka consumer is not initialized")
)

// NewConsumer creates new kafka consumer queue.
func NewConsumer(name string, config ConsumerConfig, ac bool, logger logger.Logger) queue.Consumer {
	logger = logger.ContextualLogger(map[string]interface{}{"context": name})
	return &consumer{
		name:       name,
		config:     config,
		autocommit: ac,
		logger:     logger,
	}
}

// Init consumers.
func (kc *consumer) Init() error {
	cb := confluent.NewConfluentConsumerBuilder(kc.name)

	cb.SetBroker(kc.config.ClusterAddress)
	cb.SetTopics([]string{kc.config.Topic})
	if !kc.autocommit {
		cb.DisableAutoCommit()
	}
	if kc.config.Config != nil {
		cb.SetConfig(kc.config.Config)
	}

	con, err := cb.Build()
	if err != nil {
		return err
	}

	if err := con.Setup(); err != nil {
		return err
	}

	kc.consumer = con
	return nil
}

// Poll consumer within timeout.
func (kc *consumer) Poll(timeout time.Duration) ([]queue.Msg, error) {
	if kc.consumer == nil {
		return nil, ErrConsumerInit
	}

	msgs, err := kc.consumer.Poll(timeout)
	if err != nil {
		switch t := err.(type) {
		case kafka.Error:
			if t.Code() == kafka.ErrTimedOut {
				return []queue.Msg{}, nil //no messages polled
			}
		}
		return nil, err
	}
	return toMsg(msgs), nil
}

// Ack an offset.
func (kc *consumer) Ack(offset interface{}) error {
	if kc.consumer == nil {
		return ErrConsumerInit
	}

	return kc.consumer.Commit([]interface{}{offset})
}

// Nack an offset.
func (kc *consumer) Nack(offset interface{}) error {
	if kc.consumer == nil {
		return ErrConsumerInit
	}

	return kc.Ack(offset)
}

// Close kafka consumer.
func (kc *consumer) Close() error {
	if kc.consumer == nil {
		return ErrConsumerInit
	}

	return kc.consumer.Close()
}

func toMsg(msgs []confluent.Msg) []queue.Msg {
	res := make([]queue.Msg, len(msgs))

	for i, m := range msgs {
		res[i] = queue.NewMsg(m.Data, m.Offset())
	}

	return res
}
