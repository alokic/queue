//+build !test

package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	kithttp "github.com/go-kit/kit/transport/http"

	"github.com/alokic/queue/app/controller"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/httputils"
	"github.com/honestbee/gopkg/logger"
)

var (
	// ErrorUserEndpointNotFound means no matching user api routes found.
	ErrorUserEndpointNotFound = errors.New("user endpoint not found")
)

//MakeDispatcherHandler handler.
func MakeDispatcherHandler(s controller.DispatcherService, r httputils.Router, logger logger.Logger) http.Handler {
	opts := []kithttp.ServerOption{
		kithttp.ServerErrorLogger(logger),
		kithttp.ServerErrorEncoder(encodeDispatcherError),
	}

	registerHandler := kithttp.NewServer(
		RegisterJobEndPoint(s),
		decodeDispatcherRegisterJobRequest,
		encodeDispatcherResponse,
		opts...,
	)

	deregisterHandler := kithttp.NewServer(
		DeregisterJobEndPoint(s),
		decodeDispatcherDeregisterJobRequest,
		encodeDispatcherResponse,
		opts...,
	)

	syncHandler := kithttp.NewServer(
		GossipEndPoint(s),
		decodeDispatcherGossipRequest,
		encodeDispatcherResponse,
		opts...,
	)

	r.Handle(http.MethodPost, "/api/v1/dispatcher/register/", registerHandler)
	r.Handle(http.MethodPost, "/api/v1/dispatcher/deregister/", deregisterHandler)
	r.Handle(http.MethodPost, "/api/v1/dispatcher/gossip/", syncHandler)

	r.NotFound(httputils.StatusHandler{Err: ErrorUserEndpointNotFound, Code: http.StatusNotFound})
	return r
}

func decodeDispatcherRegisterJobRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return httputils.DecodeBody(request, &controller.DispatcherJobRegisterRequest{})
}

func decodeDispatcherDeregisterJobRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return httputils.DecodeBody(request, &controller.DispatcherJobDeregisterRequest{})
}

func decodeDispatcherGossipRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return httputils.DecodeBody(request, &controller.DispatcherGossipRequest{})
}

func encodeDispatcherResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	switch t := response.(type) {
	case error:
		encodeDispatcherError(ctx, t, w)
		return nil
	default:
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		return json.NewEncoder(w).Encode(response)
	}
}

func encodeDispatcherError(ctx context.Context, err error, w http.ResponseWriter) {
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
