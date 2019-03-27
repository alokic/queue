package pkg

type (
	//JobType denotes type of Job.
	JobType string

	//JobState alias.
	JobState string

	//QueueType alias.
	QueueType string

	//KafkaConfig is kafka config for queue as in job.
	KafkaConfig struct {
		ClusterAddress []string               `json:"cluster_address" mapstructure:"cluster_address" validate:"required,min=1"`
		Topic          string                 `json:"topic" mapstructure:"topic" validate:"required"`
		Config         map[string]interface{} `json:"config" mapstructure:"config"`
	}

	// QueueConfig is the configuration of queue.
	QueueConfig struct {
		Type   QueueType   `json:"type" validate:"required,oneof=kafka"`
		Config interface{} `json:"config" validate:"required"`
	}

	//RetryConfig struct.
	RetryConfig struct {
		BackOffs []uint `json:"backoffs" validate:"min=1"`
	}

	// Job struct.
	Job struct {
		ID                uint64       `json:"id"`
		Name              string       `json:"name"`
		Description       string       `json:"description"`
		Type              JobType      `json:"type"`
		State             JobState     `json:"state"`
		QueueConfig       *QueueConfig `json:"queue_config"`
		RetryConfig       *RetryConfig `json:"retry_config"`
		MaxProcessingTime int          `json:"max_processing_time"`
		Version           int          `json:"version"`
	}
)

const (
	//NoJobState denotes no job state.
	NoJobState JobState = ""
	//Active denotes job is active in controller.
	Active JobState = "active" //job is active
	//Stopped denotes job is active in controller.
	Stopped JobState = "stopped" //on DB create
	//Deprecated denotes job is Deprecated in controller.
	Deprecated JobState = "deprecated" //on DB update
	//Archived denotes job is archived in controller.
	Archived JobState = "archived" //on DB delete

	//NoQueue denotes no queue.
	NoQueue QueueType = ""
	//KafkaQueue is kafka queue.
	KafkaQueue QueueType = "kafka"

	//NoJobType denotes no job.
	NoJobType JobType = ""
	//ConsumerJob denotes consumer job.
	ConsumerJob JobType = "consumer"
	//ProducerJob denotes producer job.
	ProducerJob JobType = "producer"
)

//MergeJob merges b into a.
func MergeJob(a, b Job) Job {
	if b.ID != 0 {
		a.ID = b.ID
	}

	if b.Name != "" {
		a.Name = b.Name
	}

	if b.Description != "" {
		a.Description = b.Description
	}

	if b.Type != NoJobType {
		a.Type = b.Type
	}

	if b.State != NoJobState {
		a.State = b.State
	}

	if b.QueueConfig != nil {
		a.QueueConfig = b.QueueConfig
	}

	if b.RetryConfig != nil {
		a.RetryConfig = b.RetryConfig
	}

	if b.MaxProcessingTime != 0 {
		a.MaxProcessingTime = b.MaxProcessingTime
	}

	if b.Version != 0 {
		a.Version = b.Version
	}

	return a
}
