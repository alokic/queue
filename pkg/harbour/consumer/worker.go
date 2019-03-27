package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/alokic/queue/pkg"
	"github.com/alokic/queue/pkg/harbour"
	"github.com/honestbee/gopkg/logger"
)

//TODO maybe import pkg as . import
type (
	//Action ack/nack.
	Action uint

	//MessageAction struct.
	MessageAction struct {
		IDs    []uint64
		Action Action
	}

	worker struct {
		parCtx           context.Context
		deathChan        chan harbour.DeathNote
		job              string
		cfg              *Config
		dispatcherClient harbour.Dispatcher
		logger           logger.Logger
	}
)

const (
	ack  Action = 0
	nack Action = 1

	maxWaitMs         = 500
	mainPollStreak    = 20
	dispatcherTimeout = 20 * time.Second
)

const (
	//GracefulDeath indicates child accepts death happily.
	GracefulDeath harbour.DeathReason = 0
	//PanicDeath indicates panic in child.
	PanicDeath harbour.DeathReason = 1
)

var (
	maxBackoff = 60 * time.Second
)

func (m MessageAction) String() string {
	return fmt.Sprintf("%v -> %v", m.Action, m.IDs)
}

func safeMsgHandler(h MsgHandler) MsgHandler {
	return func(message pkg.Message) (err error) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Recoevered from Panic in mainHandler: %v", r)
				err = fmt.Errorf("panic in mainHandler: %v", r) //set err for nack
			}
		}()
		err = h(message)
		return
	}
}

func safeDLQHandler(h DLQHandler) DLQHandler {
	return func(message pkg.DLQMessage) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Recoevered from Panic in DLQHandler: %v", r)
			}
		}()
		h(message)
	}
}

//NewRealtimeIndexer constructor.
func newWorker(parCtx context.Context, dc chan harbour.DeathNote, job string, cfg *Config, disp harbour.Dispatcher, lg logger.Logger) *worker {
	logger := lg.ContextualLogger(map[string]interface{}{"context": "realtime-dispatcher", "name": job})
	cfg.MainHandler = safeMsgHandler(cfg.MainHandler) //catch panics and nack
	cfg.DLQHandler = safeDLQHandler(cfg.DLQHandler)
	return &worker{
		parCtx:           parCtx,
		deathChan:        dc,
		job:              job,
		cfg:              cfg,
		dispatcherClient: disp,
		logger:           logger,
	}
}

func (r *worker) start() {
	defer r.stop()

	if !r.mustSubscribeJob() {
		return
	}
	r.run()
}

func (r *worker) run() {
	/*
			1. Strategy:
		         We first check if there are msgs to ack/nack. If yes then we keep acking/nacking.
				 Else we poll main queue, if no msg for poll then we poll retry N -> 1 till there are msgs there.
				 The idea is that retry messages have higher priority (plus retrying in opposite order would add
				 more jobs to subsequent level). Most of the times there won't be anything in there.
				 So we dont want to waste HTTP calls and hence we poll it not very often.
				 But once we see messages there we keep draining from highest to lowest.
			 	 But how often to poll retry could only be decided based on past data and hence we should collect it.
				 So lets say there could be at max N calls to main, then retry queues.
				 If we get:
					404: we again go to subscribe.
					503: we update timer.
					500: we update timer( same as above).

				When we nack, we know we can poll from retry queues but we are not incorporating that right now.
				We also poll DLQ msgs as separate select case and notify bugsnag for every new msg.
	*/
	var (
		err           error
		actionBundles = []*MessageAction{}
		msgs          = []pkg.Message{}
	)

	dlqTick := time.NewTicker(r.cfg.DLQPollInterval)
	pollTick := make(chan struct{})
	tick := new(time.Duration)
	r.scheduleTick(tick, false, pollTick)

	for {
		select {
		case <-r.parCtx.Done():
			r.logger.Infof("shutting down as parent done")
			return
		case <-pollTick:
			backoff := false
			if len(actionBundles) > 0 {
				actionBundles, err = r.processActions(actionBundles) //process pending ack/nack first
			} else {
				pollCandidate := r.cfg.PS.Candidate(err != nil || len(msgs) == 0) //initially it will be -1
				msgs, err = r.getMsgs(pollCandidate)
				if err == nil && len(msgs) > 0 {
					actions := r.processMsgs(msgs)
					actionBundles, err = r.processActions(actions) //actionBundles is guaranteed to be empty slice here, so we overwrite it.
				}
			}
			//common error processing
			if err != nil {
				switch err.(type) {
				case harbour.ErrJobNotFound:
					r.logger.Errorf("Job/Worklet not found. Trying to re-register...")
					if !r.mustSubscribeJob() {
						return
					}
				default:
					r.logger.Errorf("Error while polling/acking/nacking, backing off..., err: %v", err)
					backoff = true
				}
			}
			r.scheduleTick(tick, backoff, pollTick) //always schedule tick
		case <-dlqTick.C:
			r.logger.Debugf("polling for DLQ msgs...")
			msgs, err := r.getDLQMsgs()
			if err != nil {
				r.logger.Errorf("error in getting DLQ msgs, err: %v", err)
			} else {
				for _, m := range msgs {
					r.cfg.DLQHandler(m)
				}
			}
		}
	}
}

func (r *worker) stop() error {
	r.logger.Info("stopping...")
	if re := recover(); re != nil {
		r.logger.Errorf("recovered from panic: %v", re)
		r.deathChan <- harbour.DeathNote{Reason: PanicDeath, Err: fmt.Sprint(re)}
	} else {
		r.deathChan <- harbour.DeathNote{Reason: GracefulDeath}
	}

	//Maybe call Unsubscribe here.
	return nil
}

//scheduleTick assumes *t != 0
func (r *worker) scheduleTick(t *time.Duration, backOff bool, ch chan struct{}) {
	if backOff {
		*t = *t * 2
		if *t >= maxBackoff {
			*t = maxBackoff
		}
	} else {
		*t = r.cfg.MsgPollInterval //reset
	}
	r.logger.Debugf("scheduling tick in: %v", *t)
	time.AfterFunc(*t, func() {
		ch <- struct{}{}
	})
}

func (r *worker) mustSubscribeJob() bool {
	pollTick := make(chan struct{})
	tick := new(time.Duration)
	r.scheduleTick(tick, false, pollTick)
LOOP:
	for {
		select {
		case <-r.parCtx.Done():
			r.logger.Infof("shutting down as parent done")
			return false
		case <-pollTick:
			if err := r.subscribeJob(); err != nil {
				r.logger.Errorf("failed to subscribe job: %v, retrying...", err)
				r.scheduleTick(tick, true, pollTick)
			} else {
				break LOOP
			}
		}
	}
	r.logger.Infof("subscribed to JOB")
	return true
}

func (r *worker) processMsgs(msgs []pkg.Message) []*MessageAction {
	actions := []*MessageAction{}

	for _, m := range msgs {
		r.logger.Infof("Processing message: %v/%v, attempt: ", m.ID, m.Name, m.Attempt)

		if err := r.cfg.MainHandler(m); err != nil {
			r.logger.Warnf("Message: [FAIL] for ID: %v, err: %v", m.ID, err)
			actions = append(actions, &MessageAction{Action: nack, IDs: []uint64{m.ID}})
		} else {
			actions = append(actions, &MessageAction{Action: ack, IDs: []uint64{m.ID}})
		}
	}
	return actionBundles(actions)
}

func actionBundles(actions []*MessageAction) []*MessageAction {
	res := []*MessageAction{}
	in := 0

	//create ack/nack bundles
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

func (r *worker) processActions(actions []*MessageAction) ([]*MessageAction, error) {
	failedIDIndex := 0
	var err error

	for i, action := range actions {
		var failedIDs []uint64
		var badIDs []uint64

		if action.Action == ack {
			r.logger.Debugf("acking: %v", action.IDs)
			failedIDs, badIDs, err = r.ack(action.IDs)
		} else {
			r.logger.Debugf("nacking: %v", action.IDs)
			failedIDs, badIDs, err = r.nack(action.IDs)
		}
		if err != nil {
			break
		}
		if len(badIDs) > 0 {
			r.logger.Errorf("Bad IDs found: %v", badIDs)
		}

		if len(failedIDs) > 0 {
			r.logger.Warnf("failed for action: %v with IDs: %v, err: %v", i, failedIDs, err)
			action.IDs = failedIDs //update FailedIDs for next turn
			break
		}
		failedIDIndex++
	}
	return actions[failedIDIndex:], err
}

func (r *worker) timeout() time.Duration {
	if r.cfg.APITimeout != 0 {
		return r.cfg.APITimeout
	}
	return dispatcherTimeout
}

func (r *worker) subscribeJob() error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout())
	defer cancel()
	_, err := r.dispatcherClient.SubscribeJob(ctx, &harbour.WorkerSubscribeJobRequest{Name: r.job})
	return err
}

func (r *worker) getMsgs(candidate int) ([]pkg.Message, error) {
	r.logger.Debugf("Polling for msgs from: %v", candidate)
	if candidate == -1 {
		return r.getMainMsgs()
	}
	return r.getRetryMsgs(uint(candidate))
}

func (r *worker) getMainMsgs() ([]pkg.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout())
	defer cancel()

	req := &harbour.WorkerGetMsgsRequest{Name: r.job, MaxWait: maxWaitMs}

	rep, err := r.dispatcherClient.GetMsgs(ctx, req)
	if err != nil {
		return nil, err
	}
	return rep.Msgs, nil
}

func (r *worker) getRetryMsgs(ri uint) ([]pkg.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout())
	defer cancel()

	req := &harbour.WorkerGetRetryMsgsRequest{Name: r.job, RetryIndex: ri, MaxWait: maxWaitMs}

	rep, err := r.dispatcherClient.GetRetryMsgs(ctx, req)
	if err != nil {
		return nil, err
	}
	return rep.Msgs, nil
}

func (r *worker) getDLQMsgs() ([]pkg.DLQMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout())
	defer cancel()

	req := &harbour.WorkerGetDLQMsgsRequest{Name: r.job, MaxWait: maxWaitMs}

	rep, err := r.dispatcherClient.GetDLQMsgs(ctx, req)
	if err != nil {
		return nil, err
	}
	return rep.Msgs, nil
}

func (r *worker) ack(ids []uint64) ([]uint64, []uint64, error) {
	if len(ids) == 0 {
		return []uint64{}, []uint64{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout())
	defer cancel()

	req := &harbour.WorkerAckMsgsRequest{Name: r.job, MsgIDs: ids}

	rep, err := r.dispatcherClient.AckMsgs(ctx, req)
	if err != nil {
		r.logger.Errorf("error in acking IDs: %v, err: %v", ids, err)
		return ids, []uint64{}, err
	}
	if rep.Error != "" {
		r.logger.Errorf("error in acking: %v", rep.Error)
	}
	return rep.FailedIDs, rep.BadIDs, nil
}

func (r *worker) nack(ids []uint64) ([]uint64, []uint64, error) {
	if len(ids) == 0 {
		return []uint64{}, []uint64{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout())
	defer cancel()

	req := &harbour.WorkerNackMsgsRequest{Name: r.job, MsgIDs: ids}

	rep, err := r.dispatcherClient.NackMsgs(ctx, req)
	if err != nil {
		r.logger.Errorf("error in nacking IDs: %v, err: %v", ids, err)
		return ids, []uint64{}, err
	}
	if rep.Error != "" {
		r.logger.Errorf("error in nacking: %v", rep.Error)
	}
	return rep.FailedIDs, rep.BadIDs, nil
}
