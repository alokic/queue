package kafka

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

type saramaProducer struct {
	topic string
	p     sarama.SyncProducer
}

//NewSaramaProducer errs when cluster is not reachable.
func NewSaramaProducer(topic string, hosts []string) (KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 5                    // Retry up to 5 times to produce the message
	config.Producer.Return.Successes = true

	kp, err := sarama.NewSyncProducer(hosts, config)
	if err != nil {
		return nil, err
	}
	return &saramaProducer{topic: topic, p: kp}, nil
}

func (p *saramaProducer) Write(msgs []json.RawMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	saramaMsgs := make([]*sarama.ProducerMessage, len(msgs))
	{
		for i, m := range msgs {
			saramaMsgs[i] = &sarama.ProducerMessage{
				Topic: p.topic,
				Value: sarama.ByteEncoder(m),
			}
		}
	}

	saramaErr := p.p.SendMessages(saramaMsgs)
	if saramaErr == nil {
		return nil
	}

	errStr := ""
	{
		switch err := saramaErr.(type) {
		case sarama.ProducerErrors:
			for _, e := range err {
				errStr += e.Error() + "\n"
			}
		default:
			errStr = err.Error()
		}
	}
	return errors.New(errStr)
}

func (p *saramaProducer) Close() error {
	return p.p.Close()
}
