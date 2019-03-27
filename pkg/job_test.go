package pkg

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMergeJob(t *testing.T) {

	tests := map[string]struct {
		a   Job
		b   Job
		out Job
	}{
		"everything gets overwritten": {
			a: Job{
				ID:                1,
				Name:              "alpha",
				Description:       "alpha",
				Type:              ConsumerJob,
				State:             Stopped,
				QueueConfig:       &QueueConfig{Type: NoQueue},
				RetryConfig:       &RetryConfig{BackOffs: []uint{}},
				MaxProcessingTime: 2,
				Version:           1,
			},
			b: Job{
				ID:                2,
				Name:              "beta",
				Description:       "beta",
				Type:              ProducerJob,
				State:             Active,
				QueueConfig:       &QueueConfig{Type: KafkaQueue},
				RetryConfig:       &RetryConfig{BackOffs: []uint{1}},
				MaxProcessingTime: 1,
				Version:           2,
			},
			out: Job{
				ID:                2,
				Name:              "beta",
				Description:       "beta",
				Type:              ProducerJob,
				State:             Active,
				QueueConfig:       &QueueConfig{Type: KafkaQueue},
				RetryConfig:       &RetryConfig{BackOffs: []uint{1}},
				MaxProcessingTime: 1,
				Version:           2,
			},
		},
		"all from a": {
			a: Job{
				ID:                1,
				Name:              "alpha",
				Description:       "alpha",
				Type:              ConsumerJob,
				State:             Stopped,
				QueueConfig:       &QueueConfig{Type: NoQueue},
				RetryConfig:       &RetryConfig{BackOffs: []uint{}},
				MaxProcessingTime: 2,
				Version:           1,
			},
			b: Job{},
			out: Job{
				ID:                1,
				Name:              "alpha",
				Description:       "alpha",
				Type:              ConsumerJob,
				State:             Stopped,
				QueueConfig:       &QueueConfig{Type: NoQueue},
				RetryConfig:       &RetryConfig{BackOffs: []uint{}},
				MaxProcessingTime: 2,
				Version:           1,
			},
		},
		"few from each: #1": {
			a: Job{
				Name:              "alpha",
				Description:       "alpha",
				Type:              ConsumerJob,
				State:             Stopped,
				QueueConfig:       &QueueConfig{Type: NoQueue},
				RetryConfig:       &RetryConfig{BackOffs: []uint{}},
				MaxProcessingTime: 2,
				Version:           1,
			},
			b: Job{
				ID:                2,
				Type:              ProducerJob,
				QueueConfig:       &QueueConfig{Type: KafkaQueue},
				MaxProcessingTime: 1,
			},
			out: Job{
				ID:                2,
				Name:              "alpha",
				Description:       "alpha",
				Type:              ProducerJob,
				State:             Stopped,
				QueueConfig:       &QueueConfig{Type: KafkaQueue},
				RetryConfig:       &RetryConfig{BackOffs: []uint{}},
				MaxProcessingTime: 1,
				Version:           1,
			},
		},
	}

	for name, test := range tests {
		t.Log("running: ", name)
		assert.Equal(t, test.out, MergeJob(test.a, test.b))
	}
}
