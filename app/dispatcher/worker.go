package dispatcher

import (
	"context"
	"github.com/go-playground/validator"
	"github.com/alokic/queue/pkg"
)

// WorkerSubscribeJobRequest request.
type WorkerSubscribeJobRequest struct {
	Name string `json:"name" validate:"required"`
}

// WorkerSubscribeJobReply reply.
type WorkerSubscribeJobReply struct{}

// WorkerUnsubscribeJobRequest request.
type WorkerUnsubscribeJobRequest struct {
	Name string `json:"name" validate:"required"`
}

// WorkerUnsubscribeJobReply reply.
type WorkerUnsubscribeJobReply struct{}

// WorkerGetMsgsRequest request.
type WorkerGetMsgsRequest struct {
	Name    string `json:"name" validate:"required"`
	MaxWait uint   `json:"max_wait_ms" validate:"required"`
}

// WorkerGetMsgsReply reply.
type WorkerGetMsgsReply struct {
	Msgs []pkg.Message `json:"msgs"`
	//Timedout
}

// WorkerGetRetryMsgsRequest request.
type WorkerGetRetryMsgsRequest struct {
	Name       string `json:"name" validate:"required"`
	RetryIndex uint   `json:"retry_queue"`
	MaxWait    uint   `json:"max_wait_ms" validate:"required"`
}

// WorkerGetRetryMsgsReply reply.
type WorkerGetRetryMsgsReply struct {
	Msgs []pkg.Message `json:"msgs"`
}

// WorkerAckMsgsRequest request.
type WorkerAckMsgsRequest struct {
	Name   string   `json:"name" validate:"required"`
	MsgIDs []uint64 `json:"msg_ids" validate:"required,min=1"`
}

// WorkerAckMsgsReply reply.
type WorkerAckMsgsReply struct {
	FailedIDs []uint64 `json:"failed_ids"`
	BadIDs    []uint64 `json:"bad_ids"`
	Error     string   `json:"error"`
}

// WorkerNackMsgsRequest request.
type WorkerNackMsgsRequest struct {
	Name   string   `json:"name" validate:"required"`
	MsgIDs []uint64 `json:"msg_ids" validate:"required,min=1"`
}

// WorkerNackMsgsReply reply.
type WorkerNackMsgsReply struct {
	FailedIDs []uint64 `json:"failed_ids"`
	BadIDs    []uint64 `json:"bad_ids"`
	Error     string   `json:"error"`
}

// WorkerGetDLQMsgsRequest request.
type WorkerGetDLQMsgsRequest struct {
	Name    string `json:"name" validate:"required"`
	MaxWait uint   `json:"max_wait_ms" validate:"required"`
}

// WorkerGetDLQMsgsReply reply.
type WorkerGetDLQMsgsReply struct {
	Msgs []pkg.DLQMessage `json:"msgs"`
}

// WorkerPublishMsgsRequest request.
type WorkerPublishMsgsRequest struct {
	Name string         `json:"name" validate:"required"`
	Msgs []*pkg.Message `json:"msgs" validate:"required,min=1"`
}

// WorkerPublishMsgsReply reply.
type WorkerPublishMsgsReply struct{}

// WorkerService defines manager as service.
type WorkerService interface {
	Init()
	SubscribeJob(context.Context, *WorkerSubscribeJobRequest) (*WorkerSubscribeJobReply, error)
	UnsubscribeJob(context.Context, *WorkerUnsubscribeJobRequest) (*WorkerUnsubscribeJobReply, error)
	GetMsgs(context.Context, *WorkerGetMsgsRequest) (*WorkerGetMsgsReply, error)
	GetRetryMsgs(context.Context, *WorkerGetRetryMsgsRequest) (*WorkerGetRetryMsgsReply, error)
	AckMsgs(context.Context, *WorkerAckMsgsRequest) (*WorkerAckMsgsReply, error)
	NackMsgs(context.Context, *WorkerNackMsgsRequest) (*WorkerNackMsgsReply, error)
	GetDLQMsgs(context.Context, *WorkerGetDLQMsgsRequest) (*WorkerGetDLQMsgsReply, error)
	PublishMsg(context.Context, *WorkerPublishMsgsRequest) (*WorkerPublishMsgsReply, error)
	Stop()
}

//Validate validates struct.
func (r *WorkerSubscribeJobRequest) Validate() error {
	return validator.New().Struct(r)
}

//Validate validates struct.
func (r *WorkerUnsubscribeJobRequest) Validate() error {
	return validator.New().Struct(r)
}

//Validate validates struct.
func (r *WorkerGetMsgsRequest) Validate() error {
	return validator.New().Struct(r)
}

//Validate validates struct.
func (r *WorkerGetRetryMsgsRequest) Validate() error {
	return validator.New().Struct(r)
}

//Validate validates struct.
func (r *WorkerAckMsgsRequest) Validate() error {
	return validator.New().Struct(r)
}

//Validate validates struct.
func (r *WorkerNackMsgsRequest) Validate() error {
	return validator.New().Struct(r)
}

//Validate validates struct.
func (r *WorkerGetDLQMsgsRequest) Validate() error {
	return validator.New().Struct(r)
}

//Validate validates struct.
func (r *WorkerPublishMsgsRequest) Validate() error {
	return validator.New().Struct(r)
}
