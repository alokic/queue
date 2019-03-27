package worker

import (
	"context"

	"github.com/go-kit/kit/endpoint"
	"github.com/alokic/queue/app/dispatcher"
)

// SubscribeEndPoint allows transport layer to poll jobs for service.
func SubscribeEndPoint(s dispatcher.WorkerService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.SubscribeJob(ctx, request.(*dispatcher.WorkerSubscribeJobRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// UnsubscribeEndPoint allows transport layer to poll jobs for service.
func UnsubscribeEndPoint(s dispatcher.WorkerService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.UnsubscribeJob(ctx, request.(*dispatcher.WorkerUnsubscribeJobRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// MessagesEndPoint allows transport layer to poll jobs for service.
func MessagesEndPoint(s dispatcher.WorkerService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.GetMsgs(ctx, request.(*dispatcher.WorkerGetMsgsRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// RetryMessagesEndPoint allows transport layer to poll jobs for service.
func RetryMessagesEndPoint(s dispatcher.WorkerService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.GetRetryMsgs(ctx, request.(*dispatcher.WorkerGetRetryMsgsRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// DLQMessagesEndPoint allows transport layer to poll jobs for service.
func DLQMessagesEndPoint(s dispatcher.WorkerService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.GetDLQMsgs(ctx, request.(*dispatcher.WorkerGetDLQMsgsRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// AckEndPoint allows transport layer to ack jobs for service.
func AckEndPoint(s dispatcher.WorkerService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.AckMsgs(ctx, request.(*dispatcher.WorkerAckMsgsRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// NackEndPoint allows transport layer to nack jobs for service.
func NackEndPoint(s dispatcher.WorkerService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.NackMsgs(ctx, request.(*dispatcher.WorkerNackMsgsRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}

// PublishEndPoint allows transport layer to publish jobs for service.
func PublishEndPoint(s dispatcher.WorkerService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		reply, err := s.PublishMsg(ctx, request.(*dispatcher.WorkerPublishMsgsRequest))
		if err != nil {
			return nil, err
		}

		return reply, nil
	}
}
