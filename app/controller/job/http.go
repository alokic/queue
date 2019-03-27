//+build !test

package job

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	kithttp "github.com/go-kit/kit/transport/http"

	"fmt"
	"github.com/gorilla/mux"
	"github.com/alokic/queue/app/controller"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/httputils"
	"github.com/honestbee/gopkg/logger"
)

var (
	// ErrorUserEndpointNotFound means no matching user api routes found.
	ErrorUserEndpointNotFound = errors.New("user endpoint not found")
)

//MakeJobHandler handler.
func MakeJobHandler(s controller.JobService, r httputils.Router, logger logger.Logger) http.Handler {
	opts := []kithttp.ServerOption{
		kithttp.ServerErrorLogger(logger),
		kithttp.ServerErrorEncoder(encodeJobError),
		kithttp.ServerBefore(urlPath),
	}

	getHandler := kithttp.NewServer(
		GetJobEndPoint(s),
		decodeGetJobRequest,
		encodeJobResponse,
		opts...,
	)

	createHandler := kithttp.NewServer(
		CreateJobEndPoint(s),
		decodeJobCreateRequest,
		encodeJobResponse,
		opts...,
	)

	updateHandler := kithttp.NewServer(
		UpdateJobEndPoint(s),
		decodeUpdateJobRequest,
		encodeJobResponse,
		opts...,
	)

	deleteHandler := kithttp.NewServer(
		ArchiveJobEndPoint(s),
		decodeJobDeleteRequest,
		encodeJobResponse,
		opts...,
	)

	startHandler := kithttp.NewServer(
		StartJobEndPoint(s),
		decodeJobStartRequest,
		encodeJobResponse,
		opts...,
	)

	stopHandler := kithttp.NewServer(
		StopJobEndPoint(s),
		decodeJobStopRequest,
		encodeJobResponse,
		opts...,
	)

	r.Handle(http.MethodGet, "/api/v1/job/{name}/", getHandler)
	r.Handle(http.MethodPost, "/api/v1/job/", createHandler)
	r.Handle(http.MethodPost, "/api/v1/job/{name}/", updateHandler)
	r.Handle(http.MethodDelete, "/api/v1/job/{name}/", deleteHandler)
	r.Handle(http.MethodPut, "/api/v1/job/{name}/start/", startHandler)
	r.Handle(http.MethodPut, "/api/v1/job/{name}/stop/", stopHandler)

	r.NotFound(httputils.StatusHandler{Err: ErrorUserEndpointNotFound, Code: http.StatusNotFound})
	return r
}

func decodeGetJobRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return request, nil
}

func decodeJobCreateRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	fmt.Println(request.Method, " ", request.URL)
	return httputils.DecodeBody(request, &controller.CreateJobRequest{})
}

func decodeUpdateJobRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return httputils.DecodeBody(request, &controller.UpdateJobRequest{})
}

func decodeJobDeleteRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return httputils.DecodeBody(request, &controller.ArchiveJobRequest{})
}

func decodeJobStartRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return &controller.StartJobRequest{}, nil
}

func decodeJobStopRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return &controller.StopJobRequest{}, nil
}

func encodeJobResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	switch t := response.(type) {
	case error:
		encodeJobError(ctx, t, w)
		return nil
	default:
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		return json.NewEncoder(w).Encode(response)
	}
}

func encodeJobError(ctx context.Context, err error, w http.ResponseWriter) {
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

func urlPath(ctx context.Context, request *http.Request) context.Context {
	vars := mux.Vars(request)
	return context.WithValue(ctx, name, vars[name])
}
