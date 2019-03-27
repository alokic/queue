package harbour

import (
	"context"
	"github.com/alokic/queue/pkg"
)

type (
	// WorkerSubscribeJobRequest request.
	WorkerSubscribeJobRequest struct {
		Name string `json:"name" validate:"required"`
	}

	// WorkerPublishJobRequest request.
	WorkerPublishJobRequest struct {
		Name string         `json:"name" validate:"required"`
		Msgs []*pkg.Message `json:"msgs"`
	}

	// WorkerSubscribeJobReply reply.
	WorkerSubscribeJobReply struct{}

	// WorkerPublishJobReply reply.
	WorkerPublishJobReply struct{}

	// WorkerUnsubscribeJobRequest request.
	WorkerUnsubscribeJobRequest struct {
		Name string `json:"name" validate:"required"`
	}

	// WorkerUnsubscribeJobReply reply.
	WorkerUnsubscribeJobReply struct{}

	// WorkerGetMsgsRequest request.
	WorkerGetMsgsRequest struct {
		Name    string `json:"name" validate:"required"`
		MaxWait uint   `json:"max_wait_ms" validate:"required"`
	}

	// WorkerGetMsgsReply reply.
	WorkerGetMsgsReply struct {
		Msgs []pkg.Message `json:"msgs"`
		//Timedout
	}

	// WorkerGetRetryMsgsRequest request.
	WorkerGetRetryMsgsRequest struct {
		Name       string `json:"name" validate:"required"`
		RetryIndex uint   `json:"retry_queue"`
		MaxWait    uint   `json:"max_wait_ms" validate:"required"`
	}

	// WorkerGetRetryMsgsReply reply.
	WorkerGetRetryMsgsReply struct {
		Msgs []pkg.Message `json:"msgs"`
	}

	// WorkerAckMsgsRequest request.
	WorkerAckMsgsRequest struct {
		Name   string   `json:"name" validate:"required"`
		MsgIDs []uint64 `json:"msg_ids" validate:"required,min=1"`
	}

	// WorkerAckMsgsReply reply.
	WorkerAckMsgsReply struct {
		FailedIDs []uint64 `json:"failed_ids"`
		BadIDs    []uint64 `json:"bad_ids"`
		Error     string   `json:"error"`
	}

	// WorkerNackMsgsRequest request.
	WorkerNackMsgsRequest struct {
		Name   string   `json:"name" validate:"required"`
		MsgIDs []uint64 `json:"msg_ids" validate:"required,min=1"`
	}

	// WorkerNackMsgsReply reply.
	WorkerNackMsgsReply struct {
		FailedIDs []uint64 `json:"failed_ids"`
		BadIDs    []uint64 `json:"bad_ids"`
		Error     string   `json:"error"`
	}

	// WorkerGetDLQMsgsRequest request.
	WorkerGetDLQMsgsRequest struct {
		Name    string `json:"name" validate:"required"`
		MaxWait uint   `json:"max_wait_ms" validate:"required"`
	}

	// WorkerGetDLQMsgsReply reply.
	WorkerGetDLQMsgsReply struct {
		Msgs []pkg.DLQMessage `json:"msgs"`
	}

	// WorkerPublishMsgsRequest request.
	WorkerPublishMsgsRequest struct{}

	// WorkerPublishMsgsReply reply.
	WorkerPublishMsgsReply struct{}

	// WorkerHealthCheckRequest request.
	WorkerHealthCheckRequest struct{}

	// WorkerHealthCheckReply reply.
	WorkerHealthCheckReply struct{}

	//Dispatcher interface for worker.
	Dispatcher interface {
		//SubscribeJob subscribes to a JOB.
		SubscribeJob(context.Context, *WorkerSubscribeJobRequest) (*WorkerSubscribeJobReply, error)
		//PublishJob publishes msgs.
		PublishJob(context.Context, *WorkerPublishJobRequest) (*WorkerPublishJobReply, error)
		//UnsubscribeJob unsubscribes a JOB.
		UnsubscribeJob(context.Context, *WorkerUnsubscribeJobRequest) (*WorkerUnsubscribeJobReply, error)
		//GetMsgs gets msgs from main topic.
		GetMsgs(context.Context, *WorkerGetMsgsRequest) (*WorkerGetMsgsReply, error)
		//GetRetryMsgs gets msgs from retry topic(s).
		GetRetryMsgs(context.Context, *WorkerGetRetryMsgsRequest) (*WorkerGetRetryMsgsReply, error)
		//GetDLQMsgs gets DLQ msgs.
		GetDLQMsgs(context.Context, *WorkerGetDLQMsgsRequest) (*WorkerGetDLQMsgsReply, error)
		//AckMsgs acks msgs.
		AckMsgs(context.Context, *WorkerAckMsgsRequest) (*WorkerAckMsgsReply, error)
		//NackMsgs nacks msgs.
		NackMsgs(context.Context, *WorkerNackMsgsRequest) (*WorkerNackMsgsReply, error)
		//HealthCheck does healthcheck on dispatcher.
		HealthCheck(context.Context) (*WorkerHealthCheckReply, error)
	}
)
