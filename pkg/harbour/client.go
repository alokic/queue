package harbour

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/honestbee/gopkg/logger"
	"net/http/httputil"
)

type (
	//ErrJobNotFound error.
	ErrJobNotFound struct {
		error
	}

	//ErrServiceUnavailable error.
	ErrServiceUnavailable struct {
		error
	}

	ClientOpt func(*client)

	client struct {
		host        string
		c           *http.Client
		logger      logger.Logger
		tracelogger logger.Logger
	}
)

var (
	defaultHeaders = map[string]string{}
)

// New is constructor. There would be one client per JOB.
func New(host string, lg logger.Logger, opts ...ClientOpt) Dispatcher {
	logger := lg.ContextualLogger(map[string]interface{}{"context": "dispatcher-client"})

	cl := &client{
		host: host,
		c: &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives:  false,
				DisableCompression: true, // indexer and dispatcher would run on localhost so no problem as such of bandwidth.
				//MaxConnsPerHost:    10,
				IdleConnTimeout: 300 * time.Second,
			},
			//Timeout:timeout, //set this higher than max_wait for GetMsgs.
		},
		logger: logger,
	}

	for _, opt := range opts {
		opt(cl)
	}
	return cl
}

//SetTrace adds trace logging for HTTP req/resp.
func SetTrace(lg logger.Logger) ClientOpt {
	return func(c *client) {
		c.tracelogger = lg
	}
}

//TODO: add confluence doc link for API behavior
//SubscribeJob to subscribe to a JOB. This must be called first before any other call.
func (c *client) SubscribeJob(ctx context.Context, r *WorkerSubscribeJobRequest) (*WorkerSubscribeJobReply, error) {
	req, err := c.prepareRequest(http.MethodPost, "/api/v1/worker/subscribe", r)
	if err != nil {
		return nil, errors.Wrap(err, "error in preparing request")
	}

	status, resp, err := c.do(ctx, req, defaultHeaders)
	if err != nil {
		return nil, errors.Wrap(err, "error in doing request")
	}

	switch status {
	case http.StatusNotFound:
		return nil, ErrJobNotFound{errors.New("job not found")}
	case http.StatusOK:
		rep := new(WorkerSubscribeJobReply)
		err := json.Unmarshal(resp, rep)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshall response")
		}
		return rep, nil
	default:
		return nil, fmt.Errorf("bad response code: %v", status)
	}
}

//PublishJob to publish a Message.
func (c *client) PublishJob(ctx context.Context, r *WorkerPublishJobRequest) (*WorkerPublishJobReply, error) {
	reqs, err := c.prepareRequest(http.MethodPost, "/api/v1/worker/publish", r)
	if err != nil {
		return nil, errors.Wrap(err, "error in preparing request")
	}

	status, resp, err := c.do(ctx, reqs, defaultHeaders)
	if err != nil {
		return nil, errors.Wrap(err, "error in doing request")
	}

	switch status {
	case http.StatusNotFound:
		return nil, ErrJobNotFound{errors.New("job not found")}
	case http.StatusOK:
		rep := new(WorkerPublishJobReply)
		err := json.Unmarshal(resp, rep)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshall response")
		}
		return rep, nil
	default:
		return nil, fmt.Errorf("bad response code: %v", status)
	}
}

//UnsubscribeJob unsubscribes a JOB.
func (c *client) UnsubscribeJob(ctx context.Context, r *WorkerUnsubscribeJobRequest) (*WorkerUnsubscribeJobReply, error) {
	req, err := c.prepareRequest(http.MethodPost, "/api/v1/worker/unsubscribe", r)
	if err != nil {
		return nil, errors.Wrap(err, "error in preparing request")
	}

	status, resp, err := c.do(ctx, req, defaultHeaders)
	if err != nil {
		return nil, errors.Wrap(err, "error in doing request")
	}

	switch status {
	case http.StatusNotFound:
		return nil, ErrJobNotFound{errors.New("job not found")}
	case http.StatusOK:
		rep := new(WorkerUnsubscribeJobReply)
		err := json.Unmarshal(resp, rep)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshall response")
		}
		return rep, nil
	default:
		return nil, fmt.Errorf("bad response code: %v", status)
	}
}

//GetMsgs from main topic.
func (c *client) GetMsgs(ctx context.Context, r *WorkerGetMsgsRequest) (*WorkerGetMsgsReply, error) {
	req, err := c.prepareRequest(http.MethodPost, "/api/v1/worker/messages", r)
	if err != nil {
		return nil, errors.Wrap(err, "error in preparing request")
	}

	status, resp, err := c.do(ctx, req, defaultHeaders)
	if err != nil {
		return nil, errors.Wrap(err, "error in doing request")
	}

	switch status {
	case http.StatusNotFound:
		return nil, ErrJobNotFound{errors.New("job not found")}
	case http.StatusServiceUnavailable:
		return nil, ErrServiceUnavailable{errors.New("service unavailable")}
	case http.StatusOK:
		rep := new(WorkerGetMsgsReply)
		err := json.Unmarshal(resp, rep)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshall response")
		}
		return rep, nil
	default:
		return nil, fmt.Errorf("bad response code: %v", status)
	}
}

//GetRetryMsgs from retry topic(s).
func (c *client) GetRetryMsgs(ctx context.Context, r *WorkerGetRetryMsgsRequest) (*WorkerGetRetryMsgsReply, error) {
	req, err := c.prepareRequest(http.MethodPost, "/api/v1/worker/messages/retry", r)
	if err != nil {
		return nil, errors.Wrap(err, "error in preparing request")
	}

	status, resp, err := c.do(ctx, req, defaultHeaders)
	if err != nil {
		return nil, errors.Wrap(err, "error in doing request")
	}

	switch status {
	case http.StatusNotFound:
		return nil, ErrJobNotFound{errors.New("job not found")}
	case http.StatusServiceUnavailable:
		return nil, ErrServiceUnavailable{errors.New("service unavailable")}
	case http.StatusOK:
		rep := new(WorkerGetRetryMsgsReply)
		err := json.Unmarshal(resp, rep)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshall response")
		}
		return rep, nil
	default:
		return nil, fmt.Errorf("bad response code: %v", status)
	}
}

//GetDLQMsgs from DLQ topic.
func (c *client) GetDLQMsgs(ctx context.Context, r *WorkerGetDLQMsgsRequest) (*WorkerGetDLQMsgsReply, error) {
	req, err := c.prepareRequest(http.MethodPost, "/api/v1/worker/messages/dlq", r)
	if err != nil {
		return nil, errors.Wrap(err, "error in preparing request")
	}

	status, resp, err := c.do(ctx, req, defaultHeaders)
	if err != nil {
		return nil, errors.Wrap(err, "error in doing request")
	}

	switch status {
	case http.StatusNotFound:
		return nil, ErrJobNotFound{errors.New("job not found")}
	case http.StatusServiceUnavailable:
		return nil, ErrServiceUnavailable{errors.New("service unavailable")}
	case http.StatusOK:
		rep := new(WorkerGetDLQMsgsReply)
		err := json.Unmarshal(resp, rep)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshall response")
		}
		return rep, nil
	default:
		return nil, fmt.Errorf("bad response code: %v", status)
	}
}

//AckMsgs to ack.
func (c *client) AckMsgs(ctx context.Context, r *WorkerAckMsgsRequest) (*WorkerAckMsgsReply, error) {
	req, err := c.prepareRequest(http.MethodPost, "/api/v1/worker/ack", r)
	if err != nil {
		return nil, errors.Wrap(err, "error in preparing request")
	}

	status, resp, err := c.do(ctx, req, defaultHeaders)
	if err != nil {
		return nil, errors.Wrap(err, "error in doing request")
	}

	switch status {
	case http.StatusNotFound:
		return nil, ErrJobNotFound{errors.New("job not found")}
	case http.StatusServiceUnavailable:
		return nil, ErrServiceUnavailable{errors.New("service unavailable")}
	case http.StatusOK:
		rep := new(WorkerAckMsgsReply)
		err := json.Unmarshal(resp, rep)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshall response")
		}
		return rep, nil
	default:
		return nil, fmt.Errorf("bad response code: %v", status)
	}
}

//NackMsgs to nack.
func (c *client) NackMsgs(ctx context.Context, r *WorkerNackMsgsRequest) (*WorkerNackMsgsReply, error) {
	req, err := c.prepareRequest(http.MethodPost, "/api/v1/worker/nack", r)
	if err != nil {
		return nil, errors.Wrap(err, "error in preparing request")
	}

	status, resp, err := c.do(ctx, req, defaultHeaders)
	if err != nil {
		return nil, errors.Wrap(err, "error in doing request")
	}

	switch status {
	case http.StatusNotFound:
		return nil, ErrJobNotFound{errors.New("job not found")}
	case http.StatusServiceUnavailable:
		return nil, ErrServiceUnavailable{errors.New("service unavailable")}
	case http.StatusOK:
		rep := new(WorkerNackMsgsReply)
		err := json.Unmarshal(resp, rep)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshall response")
		}
		return rep, nil
	default:
		return nil, fmt.Errorf("bad response code: %v", status)
	}
}

//HealthCheck for dispatcher healthcheck.
func (c *client) HealthCheck(ctx context.Context) (*WorkerHealthCheckReply, error) {
	req, err := c.prepareRequest(http.MethodGet, "/health", nil)
	if err != nil {
		return nil, errors.Wrap(err, "error in preparing request")
	}

	status, resp, err := c.do(ctx, req, defaultHeaders)
	if err != nil {
		return nil, errors.Wrap(err, "error in doing request")
	}

	switch status {
	case http.StatusOK:
		rep := new(WorkerHealthCheckReply)
		err := json.Unmarshal(resp, rep)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshall response")
		}
		return rep, nil
	default:
		return nil, fmt.Errorf("bad response code: %v", status)
	}
}

func (c *client) prepareRequest(method string, path string, payload interface{}) (*http.Request, error) {
	var body io.Reader
	if payload != nil {
		js, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewBuffer(js)
	}

	req, err := http.NewRequest(method, c.host+path, body)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func (c *client) do(ctx context.Context, req *http.Request, headers ...map[string]string) (status int, body []byte, err error) {
	req = req.WithContext(ctx)

	// Set converts key to canonicalMIMEkey X-Api-Token
	// so no need to transform key
	if len(headers) > 0 {
		for k, v := range headers[0] {
			req.Header.Set(k, v)
		}
	}

	c.dumpRequest(req)
	resp, err := c.c.Do(req)
	c.dumpResponse(resp)

	// An error is returned if caused by client policy (such as CheckRedirect),
	// or failure to speak HTTP (such as a network connectivity problem).
	// A non-2xx status code doesn't cause an error.
	// On error, any Response can be ignored. A non-nil Response with a non-nil
	// error only occurs when CheckRedirect fails, and even then the returned
	// Response.Body is already closed.
	if err != nil {
		if resp != nil {
			return resp.StatusCode, nil, err
		}
		return http.StatusInternalServerError, nil, err
	}

	// We will close only when no error as response is always closed in error case
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return http.StatusNoContent, nil, err
	}

	return resp.StatusCode, b, nil
}

// dumpRequest dumps the given HTTP request to the trace log.
func (c *client) dumpRequest(r *http.Request) {
	if c.tracelogger != nil {
		out, err := httputil.DumpRequestOut(r, true)
		if err == nil {
			c.tracelogger.Log(string(out))
		}
	}
}

// dumpResponse dumps the given HTTP response to the trace log.
func (c *client) dumpResponse(resp *http.Response) {
	if c.tracelogger != nil {
		out, err := httputil.DumpResponse(resp, true)
		if err == nil {
			c.tracelogger.Log(string(out))
		}
	}
}
