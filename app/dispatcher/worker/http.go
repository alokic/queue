package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	kithttp "github.com/go-kit/kit/transport/http"
	"github.com/alokic/queue/app/dispatcher"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/httputils"
	"github.com/honestbee/gopkg/logger"
)

var (
	// ErrorUserEndpointNotFound means no matching user api routes found.
	ErrorUserEndpointNotFound = errors.New("endpoint not found")
)

// MakeWorkerHandler create routes for worker api and returns http.Handler encapsulating worker routes.
func MakeWorkerHandler(ws dispatcher.WorkerService, r httputils.Router, logger logger.Logger) http.Handler {
	opts := []kithttp.ServerOption{
		kithttp.ServerErrorLogger(logger),
		kithttp.ServerErrorEncoder(encodeError),
	}

	registerHandler := kithttp.NewServer(
		SubscribeEndPoint(ws),
		decodeSubscribeRequest,
		encodeResponse, //sends response on wire
		opts...,
	)

	deregisterHandler := kithttp.NewServer(
		UnsubscribeEndPoint(ws),
		decodeUnsubscribeRequest,
		encodeResponse, //sends response on wire
		opts...,
	)

	getMsgshandler := kithttp.NewServer(
		MessagesEndPoint(ws),
		decodeMsgsRequest,
		encodeResponse, //sends response on wire
		opts...,
	)

	retryMsgshandler := kithttp.NewServer(
		RetryMessagesEndPoint(ws),
		decodeRetryMsgsRequest,
		encodeResponse, //sends response on wire
		opts...,
	)

	dlqMsgshandler := kithttp.NewServer(
		DLQMessagesEndPoint(ws),
		decodeDLQMsgsRequest,
		encodeResponse, //sends response on wire
		opts...,
	)

	ackHandler := kithttp.NewServer(
		AckEndPoint(ws),
		decodeAckRequest,
		encodeResponse,
		opts...,
	)

	nackHandler := kithttp.NewServer(
		NackEndPoint(ws),
		decodeNackRequest,
		encodeResponse,
		opts...,
	)

	publishHandler := kithttp.NewServer(
		PublishEndPoint(ws),
		decodePublishRequest,
		encodeResponse,
		opts...,
	)

	r.Handle("POST", "/api/v1/worker/subscribe", registerHandler)
	r.Handle("POST", "/api/v1/worker/unsubscribe", deregisterHandler)
	r.Handle("POST", "/api/v1/worker/messages", getMsgshandler)
	r.Handle("POST", "/api/v1/worker/messages/retry", retryMsgshandler)
	r.Handle("POST", "/api/v1/worker/messages/dlq", dlqMsgshandler)
	r.Handle("POST", "/api/v1/worker/ack", ackHandler)
	r.Handle("POST", "/api/v1/worker/nack", nackHandler)
	r.Handle("POST", "/api/v1/worker/publish", publishHandler)

	r.NotFound(httputils.StatusHandler{Err: ErrorUserEndpointNotFound, Code: http.StatusNotFound})

	return r
}

func decodeSubscribeRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return httputils.DecodeBody(request, &dispatcher.WorkerSubscribeJobRequest{})
}

func decodeUnsubscribeRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return httputils.DecodeBody(request, &dispatcher.WorkerUnsubscribeJobRequest{})
}

func decodeMsgsRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return httputils.DecodeBody(request, &dispatcher.WorkerGetMsgsRequest{})
}

func decodeRetryMsgsRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return httputils.DecodeBody(request, &dispatcher.WorkerGetRetryMsgsRequest{})
}

func decodeDLQMsgsRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return httputils.DecodeBody(request, &dispatcher.WorkerGetDLQMsgsRequest{})
}

func decodeAckRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return httputils.DecodeBody(request, &dispatcher.WorkerAckMsgsRequest{})
}

func decodeNackRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return httputils.DecodeBody(request, &dispatcher.WorkerNackMsgsRequest{})
}

func decodePublishRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return httputils.DecodeBody(request, &dispatcher.WorkerPublishMsgsRequest{})
}

func encodeResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	switch t := response.(type) {
	case error:
		encodeError(ctx, t, w)
		return nil
	default:
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		return json.NewEncoder(w).Encode(response)
	}
}

func encodeError(ctx context.Context, err error, w http.ResponseWriter) {
	var status int
	switch err.(type) {
	case pkg.ErrBadRequest:
		status = http.StatusBadRequest
	case pkg.ErrNotFound:
		status = http.StatusNotFound
	case pkg.ErrInternalServerError:
		status = http.StatusInternalServerError
	case pkg.ErrServiceUnavailable:
		status = http.StatusServiceUnavailable
	default:
		status = http.StatusInternalServerError
	}
	httputils.WriteError(err, status, w)
}
