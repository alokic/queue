package echoconsumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/alokic/queue/app/dispatcher"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/httputils"
	log "github.com/honestbee/gopkg/logger"
	"github.com/pkg/errors"
	"math/rand"
	"net/http"
	"time"
)

type (
	//Action ack/nack.
	Action uint

	//MessageAction struct.
	MessageAction struct {
		Action Action
		IDs    []uint64
	}

	consumer struct {
		name              string
		ctx               context.Context
		dispatcherAddress string
		httpClient        *http.Client
		msgPollInterval   time.Duration
		numRetryQueues    int
		dlqPollInterval   time.Duration
		logger            log.Logger
	}
)

const (
	subscribeURL   = "/api/v1/worker/subscribe"
	unsubscribeURL = "/api/v1/worker/unsubscribe"
	msgsURL        = "/api/v1/worker/messages"
	retryMsgsURL   = "/api/v1/worker/messages/retry"
	dlqMsgsURL     = "/api/v1/worker/messages/dlq"
	ackURL         = "/api/v1/worker/ack"
	nackURL        = "/api/v1/worker/nack"

	maxWait = 1000

	ack  Action = 0
	nack Action = 1

	successBias = 0.5 //fail 20% of msgs
)

//NewConsumer constructor.
func NewConsumer(ctx context.Context, name string, da string, mpi, dpi time.Duration, nrq int, lg log.Logger) *consumer {
	logger := lg.ContextualLogger(map[string]interface{}{"context": "consumer", "name": name})
	return &consumer{
		ctx:               ctx,
		name:              name,
		dispatcherAddress: da,
		numRetryQueues:    nrq,
		msgPollInterval:   mpi,
		dlqPollInterval:   dpi,
		logger:            logger,
	}
}

//Setup consumer.
func (c *consumer) Setup() error {
	//subscribe job
	url := c.dispatcherAddress + subscribeURL
	req := dispatcher.WorkerSubscribeJobRequest{Name: c.name}
	sc, _, err := httputils.Post(context.Background(), url, &req)
	if err != nil {
		return errors.Wrap(err, "error in subscribing to job")
	}
	if sc != http.StatusOK {
		return errors.New(fmt.Sprintf("bad response: %v", sc))
	}

	return nil
}

//Run consumer.
func (c *consumer) Run() {
	defer c.Stop()

	c.logger.Infof("starting with pollinterval: %v", c.msgPollInterval)
	/*
	  Poll strategy:
	  There could be numerous poll strategies to choose from. Some of them are:
	  - highest priority to retry messages and then main topic messages.
	  - round robin.
	*/

	tickMsg := time.NewTicker(c.msgPollInterval)
	pollCandidate := -1 //main queue to begin with
	actionBundles := []MessageAction{}

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info("stopping as parent done")
			c.processActions(actionBundles) //one last try
			return
		case <-tickMsg.C:
			/*
			   Note that there could be N strategies for this. the below is just one of them
			   which would always ack/nack any leftover before new polls.
			*/
			if len(actionBundles) > 0 {
				c.logger.Info("unacked messages found")
				actionBundles = c.processActions(actionBundles)
				break
			}
			msgs, err := c.getMsgs(pollCandidate)
			c.logger.Infof("polled messages: %v", msgs)
			if err != nil {
				c.logger.Warnf("error in getting messages: %v", err)
			} else {
				actions := c.processMsgs(msgs)
				actionBundles = c.processActions(actions)
			}
			pollCandidate++
			if pollCandidate >= c.numRetryQueues {
				pollCandidate = -1
			}
		}
	}
}

//Stop consumer.
func (c *consumer) Stop() {
	url := c.dispatcherAddress + unsubscribeURL
	req := dispatcher.WorkerUnsubscribeJobRequest{Name: c.name}
	sc, _, err := httputils.Post(context.Background(), url, &req)
	if err != nil {
		c.logger.Warnf("error in unsubscribing job: %v", err)
	}
	if sc != http.StatusOK {
		c.logger.Warnf("bad response: %v", sc)
	}
}

func (c *consumer) getMsgs(candidate int) ([]pkg.Message, error) {
	if candidate == -1 {
		return c.getMainMsgs()
	}
	return c.getRetryMsgs(uint(candidate))
}

func (c *consumer) getMainMsgs() ([]pkg.Message, error) {
	url := c.dispatcherAddress + msgsURL
	req := dispatcher.WorkerGetMsgsRequest{Name: c.name, MaxWait: maxWait}
	sc, respBody, err := httputils.Post(context.Background(), url, req)
	if err != nil {
		return nil, err
	}
	if sc != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("bad response code: %v", sc))
	}

	resp := new(dispatcher.WorkerGetMsgsReply)
	if err := json.Unmarshal(respBody, resp); err != nil {
		return nil, errors.Wrap(err, "bad response body")
	}

	return resp.Msgs, nil
}

func (c *consumer) getRetryMsgs(ri uint) ([]pkg.Message, error) {
	url := c.dispatcherAddress + retryMsgsURL
	req := dispatcher.WorkerGetRetryMsgsRequest{Name: c.name, RetryIndex: ri, MaxWait: maxWait}
	sc, respBody, err := httputils.Post(context.Background(), url, req)
	if err != nil {
		return nil, err
	}
	if sc != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("bad response code: %v", sc))
	}

	resp := new(dispatcher.WorkerGetRetryMsgsReply)
	if err := json.Unmarshal(respBody, resp); err != nil {
		return nil, errors.Wrap(err, "bad response body")
	}

	return resp.Msgs, nil
}

func (c *consumer) processMsgs(msgs []pkg.Message) []MessageAction {
	actions := []MessageAction{}

	for _, m := range msgs {
		c.logger.Infof("Processing message: %v/%v/%v", m.ID, m.Attempt, m.Description)
		f := rand.Float64()
		if f < successBias {
			c.logger.Infof("Message: [SUCCESS]")
			actions = append(actions, MessageAction{Action: ack, IDs: []uint64{m.ID}})
		} else {
			c.logger.Warnf("Message: [FAIL]")
			actions = append(actions, MessageAction{Action: nack, IDs: []uint64{m.ID}})
		}
	}

	res := []MessageAction{}
	in := 0
	//now create bundles
	for i, a := range actions {
		if i == 0 {
			res = append(res, a)
		} else {
			if res[in].Action == a.Action {
				res[in].IDs = append(res[in].IDs, a.IDs...)
			} else {
				res = append(res, a)
				in++ //increment counter to last element
			}
		}
	}
	return res
}

func (c *consumer) processActions(actions []MessageAction) []MessageAction {
	failedIDIndex := 0

	for i, action := range actions {
		var failed []uint64
		if action.Action == ack {
			c.logger.Infof("acking: %v", action.IDs)
			failed = c.ack(action.IDs)

		} else {
			c.logger.Infof("nacking: %v", action.IDs)
			failed = c.nack(action.IDs)
		}

		if len(failed) > 0 {
			c.logger.Warnf("failed for action: %v with IDs: %v", i, failed)
			action.IDs = failed
			break
		}
		failedIDIndex++
	}
	return actions[failedIDIndex:]
}

func (c *consumer) ack(ids []uint64) []uint64 {
	url := c.dispatcherAddress + ackURL
	req := new(dispatcher.WorkerAckMsgsRequest)
	req.Name = c.name
	req.MsgIDs = ids
	sc, respBody, err := httputils.Post(context.Background(), url, &req)

	if err != nil || sc != http.StatusOK {
		c.logger.Warnf("In ack: err: %v, status: %v", err, sc)
		return ids
	}

	rep := new(dispatcher.WorkerAckMsgsReply)
	if err := json.Unmarshal(respBody, rep); err != nil {
		c.logger.Warnf("In ack unmarshall: err: %v", err)
		return ids
	}
	return rep.FailedIDs
}

func (c *consumer) nack(ids []uint64) []uint64 {
	url := c.dispatcherAddress + nackURL
	req := new(dispatcher.WorkerNackMsgsRequest)
	req.Name = c.name
	req.MsgIDs = ids
	sc, respBody, err := httputils.Post(context.Background(), url, &req)

	if err != nil || sc != http.StatusOK {
		c.logger.Warnf("In nack: err: %v, status: %v", err, sc)
		return ids
	}

	rep := new(dispatcher.WorkerNackMsgsReply)
	if err := json.Unmarshal(respBody, rep); err != nil {
		c.logger.Warnf("In nack unmarshall: err: %v", err)
		return ids
	}
	return rep.FailedIDs
}
