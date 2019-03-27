package consumer

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/alokic/queue/pkg/harbour"
	"github.com/honestbee/gopkg/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestRealtimeIndexer_scheduleTick(t *testing.T) {
	logger.DefLogger = logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})
	oldmaxBackoff := maxBackoff
	maxBackoff = time.Second
	defer func() {
		maxBackoff = oldmaxBackoff
	}()

	/*
		1. no backoff sets to job's poll interval.
	*/

	tests := []struct {
		name     string
		curr     time.Duration
		expected time.Duration //expected value can cause RACE, its set to expected backoff - epsilon
		backoff  bool
	}{
		{name: "no backoff sets to job's poll interval", curr: 0, expected: 90 * time.Millisecond, backoff: false},
		{name: "backoff doubles it", curr: 100 * time.Millisecond, expected: 190 * time.Millisecond, backoff: true},
		{name: "backoff doubles it", curr: 500 * time.Millisecond, expected: 990 * time.Millisecond, backoff: true},
		{name: "backoff is clamped to maxBackoff", curr: 900 * time.Millisecond, expected: 990 * time.Millisecond, backoff: true},
		{name: "backoff is clamped to maxBackoff", curr: 1900 * time.Millisecond, expected: 990 * time.Millisecond, backoff: true},
		{name: "backoff is reset", curr: 1900 * time.Millisecond, expected: 90 * time.Millisecond, backoff: false},
	}
	rti := &worker{
		cfg:    &Config{MsgPollInterval: 100 * time.Millisecond},
		logger: logger.DefLogger,
	}

	for _, test := range tests {
		t.Log("running: ", test.name)
		ch := make(chan struct{})
		curr := test.curr
		t1 := time.Now()
		rti.scheduleTick(&curr, test.backoff, ch)
		select {
		case <-time.After(time.Second * 2):
			t.Fatal("didnt get tick: ", test.name, " Maybe race.")
		case <-ch:
			assert.True(t, time.Since(t1) > test.expected, time.Since(t1))
		}
	}
}

func TestRealtimeIndexer_mustSubscribeJob(t *testing.T) {
	logger.DefLogger = logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})

	t.Run("parent done in beginning", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		disp := &harbour.MockClient{
			Err:            nil,
			NumErrs:        0,
			Subscribereply: &harbour.WorkerSubscribeJobReply{},
		}
		rti := newWorker(ctx, make(chan harbour.DeathNote), "test", &Config{MsgPollInterval: 100}, disp, logger.DefLogger)
		cancel()
		assert.False(t, rti.mustSubscribeJob())
	})

	t.Run("err -> parent done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		disp := &harbour.MockClient{
			Err:            errors.New("something happened"),
			NumErrs:        100,
			Subscribereply: &harbour.WorkerSubscribeJobReply{},
		}
		rti := newWorker(ctx, make(chan harbour.DeathNote), "test", &Config{MsgPollInterval: 100}, disp, logger.DefLogger)
		time.AfterFunc(500*time.Millisecond, func() {
			cancel()
		})
		assert.False(t, rti.mustSubscribeJob())
	})

	t.Run("err -> err -> success", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		disp := &harbour.MockClient{
			Err:            errors.New("something happened"),
			NumErrs:        2,
			Subscribereply: &harbour.WorkerSubscribeJobReply{},
		}
		rti := newWorker(ctx, make(chan harbour.DeathNote), "test", &Config{MsgPollInterval: 100}, disp, logger.DefLogger)
		assert.True(t, rti.mustSubscribeJob())
	})
}

func TestRealtimeIndexer_actionBundles(t *testing.T) {
	tests := []struct {
		name     string
		input    []*MessageAction
		expected []*MessageAction
	}{
		{
			name:     "no actions",
			input:    []*MessageAction{},
			expected: []*MessageAction{},
		},
		{
			name: "all acks",
			input: []*MessageAction{
				{IDs: []uint64{1, 2, 3}, Action: ack},
				{IDs: []uint64{4, 5, 6}, Action: ack},
				{IDs: []uint64{7}, Action: ack},
			},
			expected: []*MessageAction{
				{IDs: []uint64{1, 2, 3, 4, 5, 6, 7}, Action: ack},
			},
		},
		{
			name: "all nacks",
			input: []*MessageAction{
				{IDs: []uint64{1, 2, 3}, Action: nack},
				{IDs: []uint64{4, 5, 6}, Action: nack},
				{IDs: []uint64{7}, Action: nack},
			},
			expected: []*MessageAction{
				{IDs: []uint64{1, 2, 3, 4, 5, 6, 7}, Action: nack},
			},
		},
		{
			name: "ack -> ack -> nack -> nack -> ack -> nack",
			input: []*MessageAction{
				{IDs: []uint64{1}, Action: ack},
				{IDs: []uint64{2}, Action: ack},
				{IDs: []uint64{3}, Action: nack},
				{IDs: []uint64{4}, Action: nack},
				{IDs: []uint64{5, 6}, Action: ack},
				{IDs: []uint64{7}, Action: nack},
			},
			expected: []*MessageAction{
				{IDs: []uint64{1, 2}, Action: ack},
				{IDs: []uint64{3, 4}, Action: nack},
				{IDs: []uint64{5, 6}, Action: ack},
				{IDs: []uint64{7}, Action: nack},
			},
		},
	}

	for _, test := range tests {
		t.Log("running: ", test.name)
		output := actionBundles(test.input)
		assert.True(t, reflect.DeepEqual(test.expected, output), output)
	}
}

func TestRealtimeIndexer_processActions(t *testing.T) {
	logger.DefLogger = logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})

	tests := []struct {
		name       string
		dispClient *harbour.MockClient
		actions    []*MessageAction
		expected   []*MessageAction
	}{
		{
			name: "no actions to process",
			dispClient: &harbour.MockClient{
				Err:          nil,
				NumErrs:      0,
				AckMsgsreply: &harbour.WorkerAckMsgsReply{},
			},
			actions:  []*MessageAction{},
			expected: []*MessageAction{},
		},
		{
			name: "err at 1st bundle",
			dispClient: &harbour.MockClient{
				Err:          errors.New("something happened"),
				NumErrs:      1,
				AckMsgsreply: &harbour.WorkerAckMsgsReply{},
			},
			actions: []*MessageAction{
				{IDs: []uint64{1, 2, 3}, Action: ack},
				{IDs: []uint64{4, 5}, Action: nack},
			},
			expected: []*MessageAction{
				{IDs: []uint64{1, 2, 3}, Action: ack},
				{IDs: []uint64{4, 5}, Action: nack},
			},
		},
		{
			name: "failed IDs at 1st bundle",
			dispClient: &harbour.MockClient{
				Err:     nil,
				NumErrs: 0,
				AckMsgsreply: &harbour.WorkerAckMsgsReply{
					FailedIDs: []uint64{3},
				},
			},
			actions: []*MessageAction{
				{IDs: []uint64{1, 2, 3}, Action: ack},
				{IDs: []uint64{4, 5}, Action: nack},
			},
			expected: []*MessageAction{
				{IDs: []uint64{3}, Action: ack}, //1,2 is not there
				{IDs: []uint64{4, 5}, Action: nack},
			},
		},
		{
			name: "failed IDs at 2nd bundle",
			dispClient: &harbour.MockClient{
				Err:          nil,
				NumErrs:      0,
				AckMsgsreply: &harbour.WorkerAckMsgsReply{},
				NackMsgsreply: &harbour.WorkerNackMsgsReply{
					FailedIDs: []uint64{5},
				},
			},
			actions: []*MessageAction{
				{IDs: []uint64{1, 2, 3}, Action: ack},
				{IDs: []uint64{4, 5}, Action: nack},
			},
			expected: []*MessageAction{
				{IDs: []uint64{5}, Action: nack},
			},
		},
		{
			name: "all good",
			dispClient: &harbour.MockClient{
				Err:           nil,
				NumErrs:       0,
				AckMsgsreply:  &harbour.WorkerAckMsgsReply{},
				NackMsgsreply: &harbour.WorkerNackMsgsReply{},
			},
			actions: []*MessageAction{
				{IDs: []uint64{1, 2, 3}, Action: ack},
				{IDs: []uint64{4, 5}, Action: nack},
			},
			expected: []*MessageAction{},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, test := range tests {
		t.Log("running: ", test.name)
		rti := newWorker(ctx, make(chan harbour.DeathNote), "test", &Config{MsgPollInterval: 100}, test.dispClient, logger.DefLogger)
		out, err := rti.processActions(test.actions)
		t.Log(err)
		assert.True(t, reflect.DeepEqual(test.expected, out), out)
	}
}

func TestRealtimeIndexer_ack(t *testing.T) {
	logger.DefLogger = logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})

	tests := []struct {
		name         string
		ids          []uint64
		dispClient   *harbour.MockClient
		errExpected  bool
		expectedFail []uint64
		expectedBad  []uint64
	}{
		{
			name: "no ids to ack",
			ids:  []uint64{},
			dispClient: &harbour.MockClient{
				Err:          errors.New("something happened"),
				NumErrs:      1,
				AckMsgsreply: &harbour.WorkerAckMsgsReply{},
			},
			errExpected:  false,
			expectedFail: []uint64{},
			expectedBad:  []uint64{},
		},
		{
			name: "err in AckMsgs",
			ids:  []uint64{1, 2, 3},
			dispClient: &harbour.MockClient{
				Err:          errors.New("something happened"),
				NumErrs:      1,
				AckMsgsreply: &harbour.WorkerAckMsgsReply{},
			},
			errExpected: true,
		},
		{
			name: "200 but Error in reply",
			ids:  []uint64{1, 2, 3},
			dispClient: &harbour.MockClient{
				Err:     nil,
				NumErrs: 0,
				AckMsgsreply: &harbour.WorkerAckMsgsReply{
					FailedIDs: []uint64{3},
					BadIDs:    []uint64{2},
					Error:     "wow",
				},
			},
			errExpected:  false,
			expectedBad:  []uint64{2},
			expectedFail: []uint64{3},
		},
		{
			name: "all good",
			ids:  []uint64{1, 2, 3},
			dispClient: &harbour.MockClient{
				Err:     nil,
				NumErrs: 0,
				AckMsgsreply: &harbour.WorkerAckMsgsReply{
					BadIDs: []uint64{2},
				},
			},
			errExpected: false,
			expectedBad: []uint64{2},
		},
	}

	for _, test := range tests {
		t.Log("Running: ", test.name)
		w := &worker{dispatcherClient: test.dispClient, cfg: &Config{}, logger: logger.DefLogger}
		f, b, err := w.ack(test.ids)
		if test.errExpected {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.True(t, reflect.DeepEqual(f, test.expectedFail))
			assert.True(t, reflect.DeepEqual(b, test.expectedBad))
		}
	}
}

func TestRealtimeIndexer_nack(t *testing.T) {
	logger.DefLogger = logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})

	tests := []struct {
		name         string
		ids          []uint64
		dispClient   *harbour.MockClient
		errExpected  bool
		expectedFail []uint64
		expectedBad  []uint64
	}{
		{
			name: "no ids to nack",
			ids:  []uint64{},
			dispClient: &harbour.MockClient{
				Err:           errors.New("something happened"),
				NumErrs:       1,
				NackMsgsreply: &harbour.WorkerNackMsgsReply{},
			},
			errExpected:  false,
			expectedFail: []uint64{},
			expectedBad:  []uint64{},
		},
		{
			name: "err in NackMsgs",
			ids:  []uint64{1, 2, 3},
			dispClient: &harbour.MockClient{
				Err:           errors.New("something happened"),
				NumErrs:       1,
				NackMsgsreply: &harbour.WorkerNackMsgsReply{},
			},
			errExpected: true,
		},
		{
			name: "200 but Error in reply",
			ids:  []uint64{1, 2, 3},
			dispClient: &harbour.MockClient{
				Err:     nil,
				NumErrs: 0,
				NackMsgsreply: &harbour.WorkerNackMsgsReply{
					FailedIDs: []uint64{3},
					BadIDs:    []uint64{2},
					Error:     "wow",
				},
			},
			errExpected:  false,
			expectedBad:  []uint64{2},
			expectedFail: []uint64{3},
		},
		{
			name: "all good",
			ids:  []uint64{1, 2, 3},
			dispClient: &harbour.MockClient{
				Err:     nil,
				NumErrs: 0,
				NackMsgsreply: &harbour.WorkerNackMsgsReply{
					BadIDs: []uint64{2},
				},
			},
			errExpected: false,
			expectedBad: []uint64{2},
		},
	}

	for _, test := range tests {
		t.Log("Running: ", test.name)
		w := &worker{dispatcherClient: test.dispClient, cfg: &Config{}, logger: logger.DefLogger}
		f, b, err := w.nack(test.ids)
		if test.errExpected {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.True(t, reflect.DeepEqual(f, test.expectedFail))
			assert.True(t, reflect.DeepEqual(b, test.expectedBad))
		}
	}
}
