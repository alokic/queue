package system

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
	ErrorUserEndpointNotFound = errors.New("user endpoint not found")
)

//MakeSystemHandler handler.
func MakeSystemHandler(s dispatcher.SystemService, r httputils.Router, logger logger.Logger) http.Handler {
	opts := []kithttp.ServerOption{
		kithttp.ServerErrorLogger(logger),
		kithttp.ServerErrorEncoder(encodeSystemError),
	}

	healthCheckHandler := kithttp.NewServer(
		HealthCheckEndPoint(s),
		decodeHealthCheckRequest,
		encodeSystemResponse,
		opts...,
	)

	r.Handle(http.MethodGet, "/health", healthCheckHandler)

	r.NotFound(httputils.StatusHandler{Err: ErrorUserEndpointNotFound, Code: http.StatusNotFound})
	return r
}

func decodeHealthCheckRequest(ctx context.Context, request *http.Request) (interface{}, error) {
	return request, nil
}

func encodeSystemResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	switch t := response.(type) {
	case error:
		encodeSystemError(ctx, t, w)
		return nil
	default:
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		return json.NewEncoder(w).Encode(response)
	}
}

func encodeSystemError(ctx context.Context, err error, w http.ResponseWriter) {
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
