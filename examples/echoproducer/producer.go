package echoprodcuer

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
)

type (
	producer struct {
		name              string
		ctx               context.Context
		dispatcherAddress string
		httpClient        *http.Client
		NumMsgs           int
		logger            log.Logger
	}
)

const (
	subscribeURL   = "/api/v1/worker/subscribe"
	unsubscribeURL = "/api/v1/worker/unsubscribe"
	publishURL     = "/api/v1/worker/publish"
)

//NewProducer constructor.
func NewProducer(ctx context.Context, name string, da string, n int, lg log.Logger) *producer {
	logger := lg.ContextualLogger(map[string]interface{}{"context": "producer", "name": name})
	return &producer{
		ctx:               ctx,
		name:              name,
		dispatcherAddress: da,
		NumMsgs:           n,
		logger:            logger,
	}
}

//Setup producer.
func (p *producer) Setup() error {
	//subscribe job
	p.logger.Infof("registering job: %v", p.name)
	url := p.dispatcherAddress + subscribeURL
	req := dispatcher.WorkerSubscribeJobRequest{Name: p.name}
	sc, _, err := httputils.Post(context.Background(), url, &req)
	if err != nil {
		return errors.Wrap(err, "error in subscribing to job")
	}
	if sc != http.StatusOK {
		return errors.New(fmt.Sprintf("bad response: %v", sc))
	}

	return nil
}

//Run producer.
func (p *producer) Run() {
	defer p.Stop()

	s := fmt.Sprint(rand.Intn(100))
	msgs := []*pkg.Message{}
	for i := 0; i < p.NumMsgs; i++ {
		msgs = append(msgs, &pkg.Message{
			ID:          uint64(i),
			Name:        "echoprodcuer",
			Description: s,
			Data:        []byte(`hello`),
		})
	}
	if err := p.publishMsg(msgs); err != nil {
		p.logger.Warnf("error in publish: %v", err)
	}
}

//Stop producer.
func (p *producer) Stop() {
	url := p.dispatcherAddress + unsubscribeURL
	req := dispatcher.WorkerUnsubscribeJobRequest{Name: p.name}
	sc, _, err := httputils.Post(context.Background(), url, &req)
	if err != nil {
		p.logger.Warnf("error in unsubscribing job: %v", err)
	}
	if sc != http.StatusOK {
		p.logger.Warnf("bad response: %v", sc)
	}
}

func (p *producer) publishMsg(msgs []*pkg.Message) error {
	url := p.dispatcherAddress + publishURL
	req := dispatcher.WorkerPublishMsgsRequest{Name: p.name, Msgs: msgs}
	sc, respBody, err := httputils.Post(context.Background(), url, req)
	if err != nil {
		return err
	}
	if sc != http.StatusOK {
		return errors.New(fmt.Sprintf("bad response code: %v", sc))
	}

	resp := new(dispatcher.WorkerPublishMsgsReply)
	if err := json.Unmarshal(respBody, resp); err != nil {
		return errors.Wrap(err, "bad response body")
	}

	return nil
}
