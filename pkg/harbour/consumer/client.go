package consumer

import (
	"context"
	"github.com/alokic/queue/pkg"
	"github.com/alokic/queue/pkg/harbour"
	"github.com/honestbee/gopkg/logger"
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"time"
)

type (

	//MsgHandler handler for handling single message, on err we nack.
	MsgHandler func(pkg.Message) error

	//MsgHandler handler for handling single DLQ message.
	DLQHandler func(message pkg.DLQMessage)

	//PollStrategy for polling from main/retry queues.
	PollStrategy interface {
		Candidate(bool) int
	}

	//CountPollStrategy for polling from main/retry queues based on count.
	CountPollStrategy struct {
		//NumRetryQueues count.
		NumRetryQueues uint
		//MainPollStreak indicates how many times to poll from main queue before going to retry queue.
		MainPollStreak uint

		curr          int
		mainPollCount uint
	}

	//TimePollStrategy for polling from main/retry queues based on time.
	TimePollStrategy struct {
		//NumRetryQueues count.
		NumRetryQueues uint
		//MainPollStreakMs indicates how much time to poll from main queue before going to retry queue.
		MainPollStreakMs uint64 //should be > MsgPollInterval

		curr          int
		mainPollStart uint64
	}

	//Config for JOB. NOTE: This can change often, hence use key:val to construct it as new fields can be added.
	Config struct {
		//APITimeout is http conn timeout.
		APITimeout time.Duration
		//MsgPollInterval - how often to poll from msgs.
		MsgPollInterval time.Duration
		//DLQPollInterval - how often to poll for DLQ msgs.
		DLQPollInterval time.Duration
		//NumRetryQueues - how many retry queues in the JOB.
		NumRetryQueues int
		//PS - pollstrategy for messages.
		PS PollStrategy
		//MainHandler for handling of main messages.
		MainHandler MsgHandler
		//DLQHandler for handling of DLQ messages.
		DLQHandler DLQHandler
		//TraceLogger enables http request/response logging.
		TraceLogger logger.Logger
	}

	client struct {
		job          string
		cfg          *Config
		w            *worker
		workerDeath  chan harbour.DeathNote
		workerCancel context.CancelFunc
	}
)

func (p *CountPollStrategy) Candidate(gotMsgs bool) int {
	/*
		TODO in future, we will opitimise based on we got any msgs from curr queue,
		For example, if no msgs from main we can go to retry queue
		OR
		if we got msg from retry queue, we can drain it too.
	*/
	if p.curr == -1 {
		if p.mainPollCount == p.MainPollStreak {
			p.mainPollCount = 0
			p.curr = int(p.NumRetryQueues) - 1
			return p.curr
		}
		p.mainPollCount++
		return p.curr
	}
	p.curr--
	return p.curr
}

func (p *TimePollStrategy) Candidate(gotMsgs bool) int {
	/*
		TODO in future, we will opitimise based on we got any msgs from curr queue,
		For example, if no msgs from main we can go to retry queue
		OR
		if we got msg from retry queue, we can drain it too.
	*/
	tNow := uint64(time.Duration(time.Now().UnixNano() / 1e6))
	if p.curr == -1 {
		if p.mainPollStart+p.MainPollStreakMs < tNow {
			p.curr = int(p.NumRetryQueues) - 1
			return p.curr
		}
		return p.curr
	}
	p.curr--
	if p.curr == -1 {
		p.mainPollStart = tNow
	}
	return p.curr
}

func (c *Config) Validate() error {
	if c == nil {
		return errors.New("Config must not be nil")
	}
	if c.MsgPollInterval == 0 {
		return errors.New("must set non-zero MsgPollInterval in Config")
	}
	if c.DLQPollInterval == 0 {
		return errors.New("must set non-zero DLQPollInterval in Config")
	}
	if c.PS == nil {
		return errors.New("must set PollStrategy(PS) in Config")
	}
	switch v := c.PS.(type) {
	case *TimePollStrategy:
		if time.Duration(v.MainPollStreakMs)*time.Millisecond <= c.MsgPollInterval {
			return errors.New("MainPollStreakMs in PollStrategy should be greater than MsgPollInterval")
		}
	case *CountPollStrategy:
		if v.MainPollStreak == 0 {
			return errors.New("MainPollStreak should be > 0")
		}
	}
	if c.MainHandler == nil {
		return errors.New("must set MainHandler in Config")
	}
	if c.DLQHandler == nil {
		return errors.New("must set DLQHandler in Config")
	}

	return nil
}

//New to construct new client.
func New(dispURL, j string, c *Config, lg logger.Logger) (harbour.Consumer, error) {
	if err := c.Validate(); err != nil {
		return nil, errors.Wrapf(err, " error in Config ")
	}

	//do a deepcopy of config to avoid mutation from caller.
	cfg := new(Config)
	if err := copier.Copy(cfg, c); err != nil {
		return nil, errors.Wrapf(err, " error in copying Config")
	}

	disp := harbour.New(dispURL, lg, harbour.SetTrace(c.TraceLogger))

	dc := make(chan harbour.DeathNote)
	ctx, cancel := context.WithCancel(context.Background())
	w := newWorker(ctx, dc, j, cfg, disp, lg)

	return &client{
		job:          j,
		cfg:          cfg,
		w:            w,
		workerCancel: cancel,
		workerDeath:  dc,
	}, nil
}

//Start starts the worker.
func (c *client) Start() {
	go c.w.start()
}

//Dead returns channel notifying caller of worker death.
func (c *client) Dead() <-chan harbour.DeathNote {
	return c.workerDeath
}

//Stop stops the worker.
func (c *client) Stop(timeout time.Duration) error {
	c.workerCancel()

	select {
	case <-c.workerDeath:
		return nil
	case <-time.After(timeout):
		return errors.New("timed out")
	}
}
