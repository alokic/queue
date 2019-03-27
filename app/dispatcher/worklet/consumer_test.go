package worklet

import (
	"context"
	"encoding/json"
	"github.com/alokic/queue/app/ackmgr"
	"github.com/alokic/queue/app/dispatcher/queue"
	"github.com/alokic/queue/app/fom"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"
)

func TestConsumer_ensureID(t *testing.T) {
	//Attempt 0
	j := pkg.Message{
		Attempt: 0,
	}

	assert.Equal(t, uint64(0), j.ID)
	ensureID(&j)
	assert.NotEqual(t, uint64(0), j.ID)
	id := j.ID
	j.Attempt++

	//Now its no-op
	ensureID(&j)
	assert.Equal(t, id, j.ID)
}

func TestConsumer_getCapability(t *testing.T) {
	tests := []struct {
		name   string
		input  pkg.JobState
		output consumerCapability
	}{
		{
			name:   "no state",
			input:  pkg.NoJobState,
			output: 7,
		},
		{
			name:   "bad state",
			input:  pkg.JobState("wow"),
			output: 7,
		},
		{
			name:   "active",
			input:  pkg.Active,
			output: 7,
		},
		{
			name:   "deprecated",
			input:  pkg.Deprecated,
			output: 7,
		},
		{
			name:   "archived",
			input:  pkg.Archived,
			output: 6,
		},
		{
			name:   "stopped",
			input:  pkg.Stopped,
			output: 6,
		},
	}

	for _, test := range tests {
		t.Log("Running:", test.name)
		assert.Equal(t, test.output, getConsumerCapability(test.input))
	}
}

func TestConsumer_GetMsgs(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	rp1 := &queue.MockProducer{}
	rp1.Init()
	rp2 := &queue.MockProducer{}
	rp2.Init()
	rp := []queue.Producer{rp1, rp2}

	dlqp := &queue.MockProducer{}
	dlqp.Init()

	fom := fom.New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger)
	fom.Init()

	con := Consumer{
		capabilities:      6,
		failedInit:        true,
		mu:                sync.Mutex{},
		messageToQueue:    map[uint64]queueMeta{},
		mainMessagesCache: []pkg.Message{},
		fom:               fom,
		logger:            logger,
	}

	//ctx done
	t.Run("context done before", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		cancel()
		_, err := con.GetMsgs(ctx, 0)
		assert.Error(t, err)
		assert.Contains(t, "timedout", err.Error())
	})

	//failedinit
	t.Run("consumer failedinit", func(t *testing.T) {
		ctx := context.Background()
		_, err := con.GetMsgs(ctx, 1000)
		assert.Error(t, err)
		assert.Contains(t, "Attempt to GetMsgs on failed consumer", err.Error())
	})

	con.failedInit = false

	t.Run("consumer not capable of poll", func(t *testing.T) {
		ctx := context.Background()
		_, err := con.GetMsgs(ctx, 1000)
		assert.Error(t, err)
		assert.Contains(t, "not capable of serving poll request", err.Error())
	})

	con.capabilities = 7
	con.messageToQueue[1] = queueMeta{}
	maxUncommitedJobs = 0

	t.Run("consumer too many unacked messages", func(t *testing.T) {
		ctx := context.Background()
		_, err := con.GetMsgs(ctx, 1000)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "many unacked/unnacked messages")
	})

	delete(con.messageToQueue, 1)
	maxUncommitedJobs = 1000

	con.mainMessagesCache = append(con.mainMessagesCache, pkg.Message{Name: "alpha"}, pkg.Message{Name: "beta"})
	t.Run("consumer returns from cache", func(t *testing.T) {
		ctx := context.Background()
		assert.Equal(t, 2, len(con.mainMessagesCache))
		msgs, err := con.GetMsgs(ctx, 1000)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(msgs))
		assert.Equal(t, "alpha", msgs[0].Name)
		assert.Equal(t, "beta", msgs[1].Name)
		assert.Equal(t, 0, len(con.mainMessagesCache))
	})

	rawCon := &queue.MockConsumer{}
	rawCon.Init()
	con.mainReceiver = rawCon
	t.Run("consumer polls with timeout", func(t *testing.T) {
		ctx := context.Background()
		msgs, err := con.GetMsgs(ctx, 100)
		assert.Error(t, err)
		assert.Nil(t, msgs)
	})

	for k := range con.messageToQueue {
		delete(con.messageToQueue, k)
	}

	t.Run("all bad messages", func(t *testing.T) {
		go func() {
			<-rawCon.NackStream
			rawCon.NackReturnStream <- true
		}()
		rawCon.Msgstream <- queue.Msg{Data: []byte("aa")}
		ctx := context.Background()
		msgs, err := con.GetMsgs(ctx, 100)
		assert.NoError(t, err)
		t.Log(msgs)
		assert.Equal(t, 0, len(msgs))

		//messages should be sent to dlq
		select {
		case <-time.After(time.Second):
			t.Fatal("failed while waiting for DLQ messages")
		case <-dlqp.PublishStream:
		}
	})

	for k := range con.messageToQueue {
		delete(con.messageToQueue, k)
	}

	t.Run("good messages", func(t *testing.T) {
		j := pkg.Job{ID: 123}
		b, _ := json.Marshal(&j)
		rawCon.Msgstream <- queue.Msg{Data: b}
		ctx := context.Background()
		msgs, err := con.GetMsgs(ctx, 100)
		assert.NoError(t, err)
		t.Log(msgs)
		assert.Equal(t, 1, len(msgs))
		assert.Equal(t, 1, msgs[0].Attempt)
		assert.Equal(t, 1, len(con.messageToQueue))
	})

	//t.Run("context done after poll should cache messages", func(t *testing.T) {
	//	j := pkg.Job{}
	//	b, _ := json.Marshal(&j)
	//	rawCon.Msgstream <- queue.Msg{Data: b}
	//	ctx, cancel := context.WithTimeout(context.Background(), 50 * time.Millisecond)
	//	defer cancel()
	//	_, err := con.GetMsgs(ctx, 100)
	//	assert.Error(t, err)
	//	assert.Contains(t, err.Error(), "timedout")
	//	t.Log(err)
	//})
}

func TestConsumer_GetRetryMsgs(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	rp1 := &queue.MockProducer{}
	rp1.Init()
	rp2 := &queue.MockProducer{}
	rp2.Init()
	rp := []queue.Producer{rp1, rp2}

	dlqp := &queue.MockProducer{}
	dlqp.Init()

	fom := fom.New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger)
	fom.Init()

	con := Consumer{
		capabilities:       6,
		failedInit:         true,
		mu:                 sync.Mutex{},
		messageToQueue:     map[uint64]queueMeta{},
		retryMessagesCache: map[uint][]pkg.Message{},
		fom:                fom,
		logger:             logger,
	}

	rCon1 := &queue.MockConsumer{}
	rCon1.Init()

	rCon2 := &queue.MockConsumer{}
	rCon2.Init()
	con.retryReceivers = []queue.Consumer{rCon1, rCon2}

	//ctx done
	t.Run("context done before", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		cancel()
		_, err := con.GetRetryMsgs(ctx, 0, 0)
		assert.Error(t, err)
		assert.Contains(t, "timedout", err.Error())
	})

	//failedinit
	t.Run("consumer failedinit", func(t *testing.T) {
		ctx := context.Background()
		_, err := con.GetRetryMsgs(ctx, 0, 0)
		assert.Error(t, err)
		assert.Contains(t, "Attempt to GetRetryMsgs on failed consumer", err.Error())
	})

	con.failedInit = false

	t.Run("consumer not capable of poll", func(t *testing.T) {
		ctx := context.Background()
		_, err := con.GetRetryMsgs(ctx, 0, 0)
		assert.Error(t, err)
		assert.Contains(t, "not capable of serving poll request", err.Error())
	})

	con.capabilities = 7
	con.messageToQueue[1] = queueMeta{}
	maxUncommitedJobs = 0

	t.Run("consumer too many unacked messages", func(t *testing.T) {
		ctx := context.Background()
		_, err := con.GetRetryMsgs(ctx, 0, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "many unacked/unnacked messages")
	})

	delete(con.messageToQueue, 1)
	maxUncommitedJobs = 1000

	t.Run("wrong retry index", func(t *testing.T) {
		ctx := context.Background()
		_, err := con.GetRetryMsgs(ctx, 2, 0)
		assert.Error(t, err)
		t.Log(err)
		assert.Contains(t, err.Error(), "bad retryIndex")
	})

	con.retryMessagesCache[0] = append(con.retryMessagesCache[0], pkg.Message{Name: "alpha"}, pkg.Message{Name: "beta"})
	t.Run("consumer returns from cache", func(t *testing.T) {
		ctx := context.Background()
		assert.Equal(t, 2, len(con.retryMessagesCache[0]))
		msgs, err := con.GetRetryMsgs(ctx, 0, 1000)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(msgs))
		assert.Equal(t, "alpha", msgs[0].Name)
		assert.Equal(t, "beta", msgs[1].Name)
		assert.Equal(t, 0, len(con.retryMessagesCache[0]))
	})

	t.Run("consumer polls with timeout", func(t *testing.T) {
		ctx := context.Background()
		msgs, err := con.GetRetryMsgs(ctx, 0, 100)
		assert.Error(t, err)
		assert.Nil(t, msgs)
	})

	for k := range con.messageToQueue {
		delete(con.messageToQueue, k)
	}

	t.Run("all bad messages", func(t *testing.T) {
		go func() {
			<-rCon2.NackStream
			rCon2.NackReturnStream <- true
		}()
		rCon2.Msgstream <- queue.Msg{Data: []byte("aa")}
		ctx := context.Background()
		msgs, err := con.GetRetryMsgs(ctx, 1, 100)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(msgs))

		//messages should be sent to dlq
		select {
		case <-time.After(time.Second):
			t.Fatal("failed while waiting for DLQ messages")
		case <-dlqp.PublishStream:
		}
	})

	for k := range con.messageToQueue {
		delete(con.messageToQueue, k)
	}

	t.Run("good messages", func(t *testing.T) {
		j := pkg.Job{ID: 123}
		b, _ := json.Marshal(&j)
		rCon2.Msgstream <- queue.Msg{Data: b}
		ctx := context.Background()
		msgs, err := con.GetRetryMsgs(ctx, 1, 100)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(msgs))
		assert.Equal(t, 1, msgs[0].Attempt)
		assert.Equal(t, 1, len(con.messageToQueue))
	})

	//t.Run("context done after poll should cache messages", func(t *testing.T) {
	//	j := pkg.Job{}
	//	b, _ := json.Marshal(&j)
	//	rawCon.Msgstream <- queue.Msg{Data: b}
	//	ctx, cancel := context.WithTimeout(context.Background(), 50 * time.Millisecond)
	//	defer cancel()
	//	_, err := con.GetMsgs(ctx, 100)
	//	assert.Error(t, err)
	//	assert.Contains(t, err.Error(), "timedout")
	//	t.Log(err)
	//})
}

func TestConsumer_GetDLQMsgs(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	rp1 := &queue.MockProducer{}
	rp1.Init()
	rp2 := &queue.MockProducer{}
	rp2.Init()
	rp := []queue.Producer{rp1, rp2}

	dlqp := &queue.MockProducer{}
	dlqp.Init()

	fom := fom.New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger)
	fom.Init()

	con := Consumer{
		capabilities:     6,
		failedInit:       true,
		mu:               sync.Mutex{},
		messageToQueue:   map[uint64]queueMeta{},
		dlqMessagesCache: []pkg.DLQMessage{},
		fom:              fom,
		logger:           logger,
	}

	//ctx done
	t.Run("context done before", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		cancel()
		_, err := con.GetDLQMsgs(ctx, 0)
		assert.Error(t, err)
		assert.Contains(t, "timedout", err.Error())
	})

	//failedinit
	t.Run("consumer failedinit", func(t *testing.T) {
		ctx := context.Background()
		_, err := con.GetDLQMsgs(ctx, 1000)
		assert.Error(t, err)
		assert.Contains(t, "Attempt to GetDLQMsgs on failed consumer", err.Error())
	})

	con.failedInit = false

	con.dlqMessagesCache = append(con.dlqMessagesCache, pkg.DLQMessage{}, pkg.DLQMessage{})
	t.Run("consumer returns from cache", func(t *testing.T) {
		ctx := context.Background()
		assert.Equal(t, 2, len(con.dlqMessagesCache))
		msgs, err := con.GetDLQMsgs(ctx, 1000)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(msgs))
		assert.Equal(t, 0, len(con.dlqMessagesCache))
	})

	rawCon := &queue.MockConsumer{}
	rawCon.Init()
	con.dlqReceiver = rawCon
	t.Run("consumer polls with timeout", func(t *testing.T) {
		ctx := context.Background()
		msgs, err := con.GetDLQMsgs(ctx, 100)
		assert.Error(t, err)
		assert.Nil(t, msgs)
	})
	for k := range con.messageToQueue {
		delete(con.messageToQueue, k)
	}

	t.Run("all bad messages", func(t *testing.T) {
		rawCon.Msgstream <- queue.Msg{Data: []byte("aa")}
		ctx := context.Background()
		msgs, err := con.GetDLQMsgs(ctx, 100)
		assert.NoError(t, err)
		t.Log(msgs)
		assert.Equal(t, 0, len(msgs))
	})

	for k := range con.messageToQueue {
		delete(con.messageToQueue, k)
	}

	t.Run("good messages", func(t *testing.T) {
		j := pkg.DLQMessage{RawJob: []byte("alp")}
		b, _ := json.Marshal(&j)
		rawCon.Msgstream <- queue.Msg{Data: b}
		ctx := context.Background()
		msgs, err := con.GetDLQMsgs(ctx, 100)
		assert.NoError(t, err)
		t.Log(msgs)
		assert.Equal(t, 1, len(msgs))
		assert.Equal(t, 0, len(con.messageToQueue))
	})

	//t.Run("context done after poll should cache messages", func(t *testing.T) {
	//	j := pkg.Job{}
	//	b, _ := json.Marshal(&j)
	//	rawCon.Msgstream <- queue.Msg{Data: b}
	//	ctx, cancel := context.WithTimeout(context.Background(), 50 * time.Millisecond)
	//	defer cancel()
	//	_, err := con.GetMsgs(ctx, 100)
	//	assert.Error(t, err)
	//	assert.Contains(t, err.Error(), "timedout")
	//	t.Log(err)
	//})
}

func TestConsumer_Ack(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	rp1 := &queue.MockProducer{}
	rp2 := &queue.MockProducer{}
	rp := []queue.Producer{rp1, rp2}
	dlqp := &queue.MockProducer{}
	fom := fom.New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger)
	fom.Init()

	ackP := &queue.MockProducer{}
	am := ackmgr.New("test", ackP, logger)
	assert.NoError(t, am.Init())

	con := Consumer{
		capabilities:   poll,
		failedInit:     true,
		mu:             sync.Mutex{},
		messageToQueue: map[uint64]queueMeta{},
		fom:            fom,
		ackmgr:         am,
		logger:         logger,
	}
	j := pkg.Job{ID: 123}
	b, _ := json.Marshal(&j)

	//ctx done
	t.Run("context done before", func(t *testing.T) {
		t.Log("Running ", t.Name())
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		cancel()
		fIDs, badIDs, err := con.Ack(ctx, []uint64{1})
		assert.Error(t, err)
		assert.Nil(t, fIDs)
		assert.Nil(t, badIDs)
		assert.Contains(t, "timedout", err.Error())
	})

	//failedinit
	t.Run("consumer failedinit", func(t *testing.T) {
		t.Log("Running ", t.Name())
		ctx := context.Background()
		fIDs, badIDs, err := con.Ack(ctx, []uint64{1})
		assert.Error(t, err)
		assert.Nil(t, fIDs)
		assert.Nil(t, badIDs)
		assert.Contains(t, "Attempt to Ack on failed consumer", err.Error())
	})

	con.failedInit = false

	t.Run("consumer not capable", func(t *testing.T) {
		t.Log("Running ", t.Name())
		ctx := context.Background()
		fIDs, badIDs, err := con.Ack(ctx, []uint64{1})
		assert.Error(t, err)
		assert.Nil(t, fIDs)
		assert.Nil(t, badIDs)
		assert.Contains(t, "not capable of serving ack request", err.Error())
	})

	con.capabilities = con.capabilities | ack

	t.Run("consumer all bad ids", func(t *testing.T) {
		t.Log("Running ", t.Name())
		ctx := context.Background()
		fIDs, badIDs, err := con.Ack(ctx, []uint64{1, 2})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(fIDs))
		assert.Equal(t, 2, len(badIDs))
	})

	rawCon := &queue.MockConsumer{}
	rawCon.Init()
	con.mainReceiver = rawCon

	t.Run("consumer bad ids + good ids", func(t *testing.T) {
		t.Log("Running ", t.Name())
		con.messageToQueue[2] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		ctx := context.Background()
		go func() {
			<-rawCon.AckStream
			rawCon.AckReturnStream <- true
		}()
		fIDs, badIDs, err := con.Ack(ctx, []uint64{1, 2, 3})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(fIDs))
		assert.Equal(t, []uint64{1, 3}, badIDs)
		assert.Equal(t, 0, len(con.messageToQueue))
	})

	for k := range con.messageToQueue {
		delete(con.messageToQueue, k)
	}

	t.Run("consumer bad ids + failed IDs", func(t *testing.T) {
		t.Log("Running ", t.Name())
		con.messageToQueue[2] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[3] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[4] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		ctx := context.Background()
		go func() {
			for i := 0; i < 2; i++ {
				<-rawCon.AckStream
				rawCon.AckReturnStream <- true
			}
			<-rawCon.AckStream
			rawCon.AckReturnStream <- false
		}()
		fIDs, badIDs, err := con.Ack(ctx, []uint64{1, 2, 3, 4})
		assert.Error(t, err)
		assert.Equal(t, []uint64{4}, fIDs)
		assert.Equal(t, []uint64{1}, badIDs)
		assert.Equal(t, 1, len(con.messageToQueue))
	})

	for k := range con.messageToQueue {
		delete(con.messageToQueue, k)
	}

	t.Run("all failed IDs", func(t *testing.T) {
		t.Log("Running ", t.Name())
		con.messageToQueue[1] = queueMeta{q: rawCon, msg: queue.Msg{}}
		con.messageToQueue[2] = queueMeta{q: rawCon, msg: queue.Msg{}}
		con.messageToQueue[3] = queueMeta{q: rawCon, msg: queue.Msg{}}
		con.messageToQueue[4] = queueMeta{q: rawCon, msg: queue.Msg{}}
		ctx := context.Background()
		go func() {
			<-rawCon.AckStream
			rawCon.AckReturnStream <- false
		}()
		fIDs, badIDs, err := con.Ack(ctx, []uint64{1, 2, 3, 4})
		assert.Error(t, err)
		assert.Equal(t, []uint64{1, 2, 3, 4}, fIDs)
		assert.Equal(t, []uint64{}, badIDs)
		assert.Equal(t, 4, len(con.messageToQueue))
	})

	for k := range con.messageToQueue {
		delete(con.messageToQueue, k)
	}

	t.Run("success, failed, bad IDs", func(t *testing.T) {
		t.Log("Running ", t.Name())
		con.messageToQueue[1] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[3] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[4] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		ctx := context.Background()
		go func() {
			<-rawCon.AckStream
			rawCon.AckReturnStream <- true
			<-rawCon.AckStream
			rawCon.AckReturnStream <- true
			<-rawCon.AckStream
			rawCon.AckReturnStream <- false
		}()
		fIDs, badIDs, err := con.Ack(ctx, []uint64{1, 2, 3, 4})
		assert.Error(t, err)
		assert.Equal(t, []uint64{4}, fIDs)
		assert.Equal(t, []uint64{2}, badIDs)
	})

	t.Run("success then timeout", func(t *testing.T) {
		t.Log("Running ", t.Name())
		con.messageToQueue[1] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[2] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[3] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[4] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		go func() {
			<-rawCon.AckStream
			time.Sleep(200 * time.Millisecond)
			rawCon.AckReturnStream <- true
		}()
		fIDs, badIDs, err := con.Ack(ctx, []uint64{1, 2, 3, 4})
		assert.Error(t, err)
		assert.Equal(t, []uint64{2, 3, 4}, fIDs)
		assert.Equal(t, []uint64{}, badIDs)
	})

}

func TestConsumer_Nack(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	rp1 := &queue.MockProducer{}
	rp1.Init()
	rp2 := &queue.MockProducer{}
	rp2.Init()
	rp := []queue.Producer{rp1, rp2}

	dlqp := &queue.MockProducer{}
	dlqp.Init()

	fom := fom.New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger)
	fom.Init()

	con := Consumer{
		capabilities:   poll,
		failedInit:     true,
		mu:             sync.Mutex{},
		messageToQueue: map[uint64]queueMeta{},
		fom:            fom,
		logger:         logger,
	}

	//ctx done
	t.Run("context done before", func(t *testing.T) {
		t.Log("Running ", t.Name())
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		cancel()
		fIDs, badIDs, err := con.Nack(ctx, []uint64{1})
		assert.Error(t, err)
		assert.Nil(t, fIDs)
		assert.Nil(t, badIDs)
		assert.Contains(t, "timedout", err.Error())
	})

	//failedinit
	t.Run("consumer failedinit", func(t *testing.T) {
		t.Log("Running ", t.Name())
		ctx := context.Background()
		fIDs, badIDs, err := con.Nack(ctx, []uint64{1})
		assert.Error(t, err)
		assert.Nil(t, fIDs)
		assert.Nil(t, badIDs)
		assert.Contains(t, "Attempt to Nack on failed consumer", err.Error())
	})

	con.failedInit = false

	t.Run("consumer not capable", func(t *testing.T) {
		t.Log("Running ", t.Name())
		ctx := context.Background()
		fIDs, badIDs, err := con.Nack(ctx, []uint64{1})
		assert.Error(t, err)
		assert.Nil(t, fIDs)
		assert.Nil(t, badIDs)
		assert.Contains(t, "not capable of serving nack request", err.Error())
	})

	con.capabilities = con.capabilities | nack

	t.Run("consumer all bad ids", func(t *testing.T) {
		t.Log("Running ", t.Name())
		ctx := context.Background()
		fIDs, badIDs, err := con.Nack(ctx, []uint64{1, 2})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(fIDs))
		assert.Equal(t, 2, len(badIDs))
	})

	rawCon := &queue.MockConsumer{}
	rawCon.Init()
	con.mainReceiver = rawCon

	j := pkg.Job{ID: 123}
	b, _ := json.Marshal(&j)
	t.Run("consumer bad ids + good ids", func(t *testing.T) {
		t.Log("Running ", t.Name())
		con.messageToQueue[2] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		ctx := context.Background()
		go func() {
			<-rawCon.NackStream
			rawCon.NackReturnStream <- true
		}()
		fIDs, badIDs, err := con.Nack(ctx, []uint64{1, 2, 3})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(fIDs))
		assert.Equal(t, []uint64{1, 3}, badIDs)
		assert.Equal(t, 0, len(con.messageToQueue))
	})

	for k := range con.messageToQueue {
		delete(con.messageToQueue, k)
	}

	t.Run("consumer bad ids + failed IDs", func(t *testing.T) {
		t.Log("Running ", t.Name())
		con.messageToQueue[2] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[3] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[4] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		ctx := context.Background()
		go func() {
			for i := 0; i < 2; i++ {
				<-rawCon.NackStream
				rawCon.NackReturnStream <- true
			}
			<-rawCon.NackStream
			rawCon.NackReturnStream <- false
		}()
		fIDs, badIDs, err := con.Nack(ctx, []uint64{1, 2, 3, 4})
		assert.Error(t, err)
		t.Log(err)
		assert.Equal(t, []uint64{4}, fIDs)
		assert.Equal(t, []uint64{1}, badIDs)
		assert.Equal(t, 1, len(con.messageToQueue))
	})

	for k := range con.messageToQueue {
		delete(con.messageToQueue, k)
	}

	t.Run("all failed IDs", func(t *testing.T) {
		t.Log("Running ", t.Name())
		con.messageToQueue[1] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[2] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[3] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[4] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		ctx := context.Background()
		go func() {
			<-rawCon.NackStream
			rawCon.NackReturnStream <- false
		}()
		fIDs, badIDs, err := con.Nack(ctx, []uint64{1, 2, 3, 4})
		assert.Error(t, err)
		assert.Equal(t, []uint64{1, 2, 3, 4}, fIDs)
		assert.Equal(t, []uint64{}, badIDs)
		assert.Equal(t, 4, len(con.messageToQueue))
	})

	for k := range con.messageToQueue {
		delete(con.messageToQueue, k)
	}

	t.Run("success, failed, bad IDs", func(t *testing.T) {
		t.Log("Running ", t.Name())
		con.messageToQueue[1] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[3] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[4] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		ctx := context.Background()
		go func() {
			<-rawCon.NackStream
			rawCon.NackReturnStream <- true
			<-rawCon.NackStream
			rawCon.NackReturnStream <- true
			<-rawCon.NackStream
			rawCon.NackReturnStream <- false
		}()
		fIDs, badIDs, err := con.Nack(ctx, []uint64{1, 2, 3, 4})
		assert.Error(t, err)
		assert.Equal(t, []uint64{4}, fIDs)
		assert.Equal(t, []uint64{2}, badIDs)
	})

	t.Run("success and timeout", func(t *testing.T) {
		t.Log("Running ", t.Name())
		con.messageToQueue[1] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[2] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[3] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		con.messageToQueue[4] = queueMeta{q: rawCon, msg: queue.Msg{Data: b}}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		go func() {
			<-rawCon.NackStream
			time.Sleep(200 * time.Millisecond)
			rawCon.NackReturnStream <- true
		}()
		fIDs, badIDs, err := con.Nack(ctx, []uint64{1, 2, 3, 4})
		assert.Error(t, err)
		assert.Equal(t, []uint64{2, 3, 4}, fIDs)
		assert.Equal(t, []uint64{}, badIDs)
	})
}

func TestConsumer_HandleSync(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	con := Consumer{
		capabilities: poll,
		failedInit:   true,
		mu:           sync.Mutex{},
		logger:       logger,
		job:          &pkg.Job{State: pkg.Stopped},
	}

	t.Run("job change", func(t *testing.T) {
		j := &pkg.Job{
			State: pkg.Stopped,
		}
		con.HandleSync(context.Background(), j)
		assert.Equal(t, con.capabilities, ack|nack)
	})

}
