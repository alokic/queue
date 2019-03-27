// Package remote is for remote communication.
package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/alokic/queue/app/dispatcher/worker"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/httputils"
	"github.com/honestbee/gopkg/logger"
	"github.com/pkg/errors"
	"time"
)

const (
	registerPath   = "/api/v1/dispatcher/register/"
	deregisterPath = "/api/v1/dispatcher/deregister/"
	gossipPath     = "/api/v1/dispatcher/gossip/"
)

var (
	reqTimeout = 5 * time.Second
)

// Controller svc client
type Controller struct {
	masterURL string
	logger    logger.Logger
}

type (
	//DispatcherJobRegisterRequest request.
	DispatcherJobRegisterRequest struct {
		DispatcherID uint64 `json:"dispatcher_id"`
		JobName      string `json:"job_name"`
	}

	//DispatcherJobRegisterReply request.
	DispatcherJobRegisterReply struct {
		Job *pkg.Job `json:"job"`
	}

	//DispatcherJobDeregisterRequest request.
	DispatcherJobDeregisterRequest struct {
		DispatcherID uint64 `json:"dispatcher_id"`
		JobID        uint64 `json:"job_id"`
	}

	//DispatcherJobDeregisterReply request.
	DispatcherJobDeregisterReply struct{}

	//DispatcherGossipRequest request.
	DispatcherGossipRequest struct {
		DispatcherID uint64 `json:"dispatcher_id"`
	}

	//DispatcherGossipReply request.
	DispatcherGossipReply struct {
		Jobs []*pkg.Job `json:"jobs"`
	}
)

// NewController is constructor for controller svc client.
func NewController(mURL string, lg logger.Logger) worker.ControllerSvc {
	logger := lg.ContextualLogger(map[string]interface{}{"context": "controllerStub"})

	return &Controller{
		masterURL: mURL,
		logger:    logger,
	}
}

// Gossip of subscribed jobs from controller.
func (c *Controller) Gossip(dispatcherID uint64) ([]*pkg.Job, error) {
	URL := c.masterURL + gossipPath
	syncReq := DispatcherGossipRequest{DispatcherID: dispatcherID}

	ctx, cancel := context.WithTimeout(context.Background(), reqTimeout)
	defer cancel()

	sc, resp, err := httputils.Post(ctx, URL, syncReq)
	if err != nil {
		return nil, errors.Wrap(err, "error in making sync request")
	}
	if sc != 200 {
		return nil, errors.New(fmt.Sprintf("bad status code: %v", sc))
	}

	syncReply := new(DispatcherGossipReply)
	if err := json.Unmarshal(resp, syncReply); err != nil {
		return nil, errors.Wrap(err, "error in unmarshalling response")
	}

	return syncReply.Jobs, nil
}

// Register with controller.
func (c *Controller) Register(dispatcherID uint64, jobname string) (*pkg.Job, error) {
	URL := c.masterURL + registerPath
	registerReq := DispatcherJobRegisterRequest{DispatcherID: dispatcherID, JobName: jobname}

	ctx, cancel := context.WithTimeout(context.Background(), reqTimeout)
	defer cancel()
	sc, resp, err := httputils.Post(ctx, URL, registerReq)
	if err != nil {
		return nil, errors.Wrap(err, "error in making register request")
	}

	switch sc {
	case 200:
		registerReply := new(DispatcherJobRegisterReply)
		if err := json.Unmarshal(resp, registerReply); err != nil {
			return nil, errors.Wrap(err, "error in unmarshalling response")
		}

		return registerReply.Job, nil
	case 404:
		return nil, pkg.ErrNotFound{Err: errors.New("job not found")}
	default:
		return nil, errors.New(fmt.Sprintf("bad status code from controller: %v", sc))
	}
}

// Deregister with controller.
func (c *Controller) Deregister(dispatcherID uint64, jobid uint64) error {
	URL := c.masterURL + deregisterPath
	deregisterReq := DispatcherJobDeregisterRequest{DispatcherID: dispatcherID, JobID: jobid}

	ctx, cancel := context.WithTimeout(context.Background(), reqTimeout)
	defer cancel()
	sc, resp, err := httputils.Post(ctx, URL, deregisterReq)
	if err != nil {
		return errors.Wrap(err, "error in making deregister request")
	}

	if sc != 200 {
		return errors.New(fmt.Sprintf("bad status code: %v", sc))
	}

	deregisterReply := new(DispatcherJobDeregisterReply)
	if err := json.Unmarshal(resp, deregisterReply); err != nil {
		return errors.Wrap(err, "error in unmarshalling response")
	}

	return nil
}
