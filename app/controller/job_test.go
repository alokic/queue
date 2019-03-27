package controller

import (
	"github.com/alokic/queue/pkg"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestCreateJobRequest(t *testing.T) {
	tests := map[string]struct {
		Input          CreateJobRequest
		ExpectedErr    bool
		ExpectedErrStr string
	}{
		"no field set": {
			Input:          CreateJobRequest{},
			ExpectedErr:    true,
			ExpectedErrStr: "",
		},
		"name field not set": {
			Input: CreateJobRequest{
				Name:              "",
				Description:       "dscription",
				Type:              pkg.ConsumerJob,
				QueueConfig:       pkg.QueueConfig{Type: pkg.KafkaQueue, Config: pkg.KafkaConfig{ClusterAddress: []string{"aa"}, Topic: "a"}},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Name' failed on the 'required' tag",
		},
		"description not set": {
			Input: CreateJobRequest{
				Name:              "alpha",
				Description:       "",
				Type:              pkg.ConsumerJob,
				QueueConfig:       pkg.QueueConfig{Type: pkg.KafkaQueue, Config: pkg.KafkaConfig{ClusterAddress: []string{"aa"}, Topic: "a"}},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Description' failed on the 'required' tag",
		},
		"type not set": {
			Input: CreateJobRequest{
				Name:        "alpha",
				Description: "alpha",
				//Type:pkg.ConsumerJob,
				QueueConfig:       pkg.QueueConfig{Type: pkg.KafkaQueue, Config: pkg.KafkaConfig{ClusterAddress: []string{"aa"}, Topic: "a"}},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Type' failed on the 'required' tag",
		},
		"type bad value set": {
			Input: CreateJobRequest{
				Name:              "alpha",
				Description:       "alpha",
				Type:              pkg.NoJobType,
				QueueConfig:       pkg.QueueConfig{Type: pkg.KafkaQueue, Config: pkg.KafkaConfig{ClusterAddress: []string{"aa"}, Topic: "a"}},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Type' failed on the 'required' tag",
		},
		"type bad value set: #2": {
			Input: CreateJobRequest{
				Name:              "alpha",
				Description:       "alpha",
				Type:              pkg.JobType("alpha"),
				QueueConfig:       pkg.QueueConfig{Type: pkg.KafkaQueue, Config: pkg.KafkaConfig{ClusterAddress: []string{"aa"}, Topic: "a"}},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Type' failed on the 'oneof' tag",
		},
		"queue config not set": {
			Input: CreateJobRequest{
				Name:        "alpha",
				Description: "alpha",
				Type:        pkg.JobType("consumer"),
				//QueueConfig:pkg.QueueConfig{Type:pkg.KafkaQueue, Config:pkg.KafkaConfig{ClusterAddress:[]string{"aa"}, Topic:"a"}},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Config' failed on the 'required' tag",
		},
		"queue config bad type": {
			Input: CreateJobRequest{
				Name:              "alpha",
				Description:       "alpha",
				Type:              pkg.JobType("consumer"),
				QueueConfig:       pkg.QueueConfig{Type: pkg.NoQueue, Config: pkg.KafkaConfig{ClusterAddress: []string{"aa"}, Topic: "a"}},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Type' failed on the 'required' tag",
		},
		"queue config bad type: #2": {
			Input: CreateJobRequest{
				Name:              "alpha",
				Description:       "alpha",
				Type:              pkg.JobType("consumer"),
				QueueConfig:       pkg.QueueConfig{Type: pkg.QueueType("radish"), Config: pkg.KafkaConfig{ClusterAddress: []string{"aa"}, Topic: "a"}},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Type' failed on the 'oneof' tag",
		},
		"queue config bad config: #1": {
			Input: CreateJobRequest{
				Name:        "alpha",
				Description: "alpha",
				Type:        pkg.JobType("consumer"),
				QueueConfig: pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: nil,
				},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Config' failed on the 'required' tag",
		},
		"queue config bad config: #2": {
			Input: CreateJobRequest{
				Name:        "alpha",
				Description: "alpha",
				Type:        pkg.JobType("consumer"),
				QueueConfig: pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: "aa",
				},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "bad kafka config",
		},
		"queue config bad config: #3": {
			Input: CreateJobRequest{
				Name:        "alpha",
				Description: "alpha",
				Type:        pkg.JobType("consumer"),
				QueueConfig: pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{},
				},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Topic' failed on the 'required' tag",
		},
		"queue config bad config: #4": {
			Input: CreateJobRequest{
				Name:        "alpha",
				Description: "alpha",
				Type:        pkg.JobType("consumer"),
				QueueConfig: pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{Topic: ""},
				},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Topic' failed on the 'required' tag",
		},
		"queue config bad config: #5": {
			Input: CreateJobRequest{
				Name:        "alpha",
				Description: "alpha",
				Type:        pkg.JobType("consumer"),
				QueueConfig: pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{Topic: "alpha"},
				},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'ClusterAddress' failed on the 'required' tag",
		},
		"queue config bad config: #6": {
			Input: CreateJobRequest{
				Name:        "alpha",
				Description: "alpha",
				Type:        pkg.JobType("consumer"),
				QueueConfig: pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{Topic: "alpha", ClusterAddress: []string{}},
				},
				MaxProcessingTime: 1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'ClusterAddress' failed on the 'min' tag",
		},
		"MaxProcessingTime not set": {
			Input: CreateJobRequest{
				Name:        "alpha",
				Description: "alpha",
				Type:        pkg.JobType("consumer"),
				QueueConfig: pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{Topic: "alpha", ClusterAddress: []string{"a", "b"}},
				},
				//MaxProcessingTime:1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'MaxProcessingTime' failed on the 'min' tag",
		},
		"MaxProcessingTime bad min value": {
			Input: CreateJobRequest{
				Name:        "alpha",
				Description: "alpha",
				Type:        pkg.JobType("consumer"),
				QueueConfig: pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{Topic: "alpha", ClusterAddress: []string{"a", "b"}},
				},
				MaxProcessingTime: -1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'MaxProcessingTime' failed on the 'min' tag",
		},
		"RetryConfig bad min value": {
			Input: CreateJobRequest{
				Name:        "alpha",
				Description: "alpha",
				Type:        pkg.JobType("consumer"),
				QueueConfig: pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{Topic: "alpha", ClusterAddress: []string{"a", "b"}},
				},
				RetryConfig:       &pkg.RetryConfig{},
				MaxProcessingTime: -1,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'BackOffs' failed on the 'min' tag",
		},
		"all good": {
			Input: CreateJobRequest{
				Name:        "alpha",
				Description: "alpha",
				Type:        pkg.JobType("consumer"),
				QueueConfig: pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{Topic: "alpha", ClusterAddress: []string{"a", "b"}},
				},
				MaxProcessingTime: 1,
			},
			ExpectedErr: false,
		},
	}

	for name, test := range tests {
		t.Log("Running: ", name)
		if test.ExpectedErr {
			err := test.Input.Validate()
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), test.ExpectedErrStr))
		}
	}
}

func TestUpdateJobRequest(t *testing.T) {
	tests := map[string]struct {
		Input          UpdateJobRequest
		ExpectedErr    bool
		ExpectedErrStr string
	}{
		"no field set": {
			Input:          UpdateJobRequest{},
			ExpectedErr:    false,
			ExpectedErrStr: "",
		},
		"description field set": {
			Input: UpdateJobRequest{
				Description: "dscription",
				//Type:pkg.ConsumerJob,
				//QueueConfig:pkg.QueueConfig{Type:pkg.KafkaQueue, Config:pkg.KafkaConfig{ClusterAddress:[]string{"aa"}, Topic:"a"}},
				//MaxProcessingTime:1,
			},
			ExpectedErr: false,
			//ExpectedErrStr: "Field validation for 'Name' failed on the 'required' tag",
		},
		"MaxProcessingTime field bad value": {
			Input: UpdateJobRequest{
				Description: "dscription",
				//QueueConfig:pkg.QueueConfig{Type:pkg.KafkaQueue, Config:pkg.KafkaConfig{ClusterAddress:[]string{"aa"}, Topic:"a"}},
				MaxProcessingTime: -11,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'MaxProcessingTime' failed on the 'min' tag",
		},
		"MaxProcessingTime field good value": {
			Input: UpdateJobRequest{
				Description: "dscription",
				//QueueConfig:pkg.QueueConfig{Type:pkg.KafkaQueue, Config:pkg.KafkaConfig{ClusterAddress:[]string{"aa"}, Topic:"a"}},
				MaxProcessingTime: 11,
			},
			ExpectedErr: false,
		},
		"QueueConfig field bad value": {
			Input: UpdateJobRequest{
				Description: "dscription",
				QueueConfig: &pkg.QueueConfig{},
				//Type:pkg.KafkaQueue,
				//Config:pkg.KafkaConfig{ClusterAddress:[]string{"aa"}, Topic:"a"}},
				MaxProcessingTime: 11,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Config' failed on the 'required' tag",
		},
		"QueueConfig field bad value: #2": {
			Input: UpdateJobRequest{
				Description: "dscription",
				QueueConfig: &pkg.QueueConfig{
					Type: pkg.QueueType("alpha"),
					//Config:pkg.KafkaConfig{ClusterAddress:[]string{"aa"}, Topic:"a"},
				},
				MaxProcessingTime: 11,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Type' failed on the 'oneof' tag",
		},
		"QueueConfig field bad value: #3": {
			Input: UpdateJobRequest{
				Description: "dscription",
				QueueConfig: &pkg.QueueConfig{
					Type: pkg.QueueType("kafka"),
					//Config:pkg.KafkaConfig{ClusterAddress:[]string{"aa"}, Topic:"a"},
				},
				MaxProcessingTime: 11,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Config' failed on the 'required' tag",
		},
		"QueueConfig field bad value: #4": {
			Input: UpdateJobRequest{
				Description: "dscription",
				QueueConfig: &pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{},
				},
				MaxProcessingTime: 11,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Topic' failed on the 'required' tag",
		},
		"QueueConfig field bad value: #5": {
			Input: UpdateJobRequest{
				Description: "dscription",
				QueueConfig: &pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{Topic: ""},
				},
				MaxProcessingTime: 11,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'Topic' failed on the 'required' tag",
		},
		"QueueConfig field bad value: #6": {
			Input: UpdateJobRequest{
				Description: "dscription",
				QueueConfig: &pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{Topic: "alpha"},
				},
				MaxProcessingTime: 11,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'ClusterAddress' failed on the 'required' tag",
		},
		"QueueConfig field bad value: #7": {
			Input: UpdateJobRequest{
				Description: "dscription",
				QueueConfig: &pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{Topic: "alpha", ClusterAddress: []string{}},
				},
				MaxProcessingTime: 11,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'ClusterAddress' failed on the 'min' tag",
		},
		"RetryConfig field bad value: #1": {
			Input: UpdateJobRequest{
				Description: "dscription",
				QueueConfig: &pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{Topic: "alpha", ClusterAddress: []string{"a"}},
				},
				RetryConfig:       &pkg.RetryConfig{},
				MaxProcessingTime: 11,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'BackOffs' failed on the 'min' tag",
		},
		"RetryConfig field bad value: #2": {
			Input: UpdateJobRequest{
				Description: "dscription",
				QueueConfig: &pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{Topic: "alpha", ClusterAddress: []string{"a"}},
				},
				RetryConfig:       &pkg.RetryConfig{BackOffs: []uint{}},
				MaxProcessingTime: 11,
			},
			ExpectedErr:    true,
			ExpectedErrStr: "Field validation for 'BackOffs' failed on the 'min' tag",
		},
		"all good": {
			Input: UpdateJobRequest{
				Description: "dscription",
				QueueConfig: &pkg.QueueConfig{
					Type:   pkg.QueueType("kafka"),
					Config: pkg.KafkaConfig{Topic: "alpha", ClusterAddress: []string{"a"}},
				},
				RetryConfig:       &pkg.RetryConfig{BackOffs: []uint{1, 2}},
				MaxProcessingTime: 11,
			},
			ExpectedErr: false,
		},
	}

	for name, test := range tests {
		t.Log("Running: ", name)
		if test.ExpectedErr {
			err := test.Input.Validate()
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), test.ExpectedErrStr))
		}
	}

}
