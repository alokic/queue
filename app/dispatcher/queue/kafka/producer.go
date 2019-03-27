package kafka

import (
	"encoding/json"
	"github.com/alokic/queue/app/dispatcher/queue"
	"github.com/honestbee/gopkg/kafka"
	"github.com/honestbee/gopkg/logger"
	"github.com/pkg/errors"
)

// ProducerConfig is kafka config for kafka producer queue.
type ProducerConfig struct {
	ClusterAddress []string               `json:"cluster_address" mapstructure:"cluster_address" validate:"required,min=1"`
	Topic          string                 `json:"topic" mapstructure:"topic" validate:"required"`
	Config         map[string]interface{} `json:"config" mapstructure:"config"`
}

type producer struct {
	name     string
	producer kafka.KafkaProducer
	config   ProducerConfig
	logger   logger.Logger
}

var (
	// ErrProducerInit means we are publishing on nil producer.
	ErrProducerInit = "failed to initialize kafka producer"

	// ErrProducerPublish means we are publishing on nil producer.
	ErrProducerPublish = errors.New("failed to publish as kafka producer is not initialized")
)

// NewProducer returns a queue.Producer.
func NewProducer(name string, config ProducerConfig, lg logger.Logger) queue.Producer {
	return &producer{
		name:   name,
		config: config,
		logger: lg,
	}
}

// Init the producer.
func (kp *producer) Init() error {
	p, err := kafka.NewSaramaProducer(kp.config.Topic, kp.config.ClusterAddress)
	if err != nil {
		return errors.Wrap(err, ErrProducerInit)
	}
	kp.producer = p
	return nil
}

// Publish message.
func (kp *producer) Publish(msgs []json.RawMessage) error {
	if kp.producer == nil {
		return ErrProducerPublish
	}
	return kp.producer.Write(msgs)
}

// Close producer
func (kp *producer) Close() error {
	if kp.producer == nil {
		return errors.New(ErrProducerInit)
	}
	return kp.producer.Close()
}
