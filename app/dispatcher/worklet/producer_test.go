package worklet

import (
	"context"
	"github.com/alokic/queue/app/dispatcher/queue"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"
)

func TestProducer_Init(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	q := &queue.MockProducer{}

	p := &producer{
		capabilities: 0,
		failedInit:   true,
		mu:           sync.Mutex{},
		queue:        q,
		logger:       logger,
		job:          &pkg.Job{Version: 1},
	}

	//failedinit
	t.Run("producer failedinit", func(t *testing.T) {
		t.Log("Running ", t.Name())
		q.FailInit = true
		err := p.Init()
		assert.Error(t, err)
		assert.True(t, p.failedInit)
	})

	t.Run("producer inited", func(t *testing.T) {
		t.Log("Running ", t.Name())
		q.FailInit = false
		err := p.Init()
		assert.NoError(t, err)
		assert.False(t, p.failedInit)
	})
}

func TestProducer_Publish(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	q := &queue.MockProducer{}
	q.Init()

	p := producer{
		capabilities: 0,
		failedInit:   true,
		mu:           sync.Mutex{},
		queue:        q,
		logger:       logger,
	}

	//failedinit
	t.Run("producer failedinit", func(t *testing.T) {
		t.Log("Running ", t.Name())
		err := p.Publish([]*pkg.Message{{ID: 1}})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "attempt to publish on failed producer")
	})

	p.failedInit = false

	t.Run("producer not capable", func(t *testing.T) {
		t.Log("Running ", t.Name())
		err := p.Publish([]*pkg.Message{{ID: 1}})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not capable of serving publish request")
	})

	p.capabilities = publish

	t.Run("queue errs", func(t *testing.T) {
		t.Log("Running ", t.Name())
		q.FailAction = true
		err := p.Publish([]*pkg.Message{{ID: 1}})
		assert.Error(t, err)
	})

	t.Run("queue ok", func(t *testing.T) {
		t.Log("Running ", t.Name())
		q.FailAction = false
		go func() {
			select {
			case <-time.After(time.Second):
				t.Fatal("timedout waiting for publish msg")
			case <-q.PublishStream:
			}
		}()
		err := p.Publish([]*pkg.Message{{ID: 1}})
		assert.NoError(t, err)
	})

}

func TestProducer_HandleSync(t *testing.T) {
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})
	p := producer{
		capabilities: publish,
		failedInit:   false,
		queue:        &queue.MockProducer{},
		mu:           sync.Mutex{},
		logger:       logger,
		job:          &pkg.Job{State: pkg.Stopped},
	}

	t.Run("job change", func(t *testing.T) {
		j := &pkg.Job{
			State: pkg.Stopped,
		}
		p.HandleSync(context.Background(), j)
		assert.Equal(t, int(p.capabilities), 0)
	})

	t.Run("job change 2", func(t *testing.T) {
		j := &pkg.Job{
			State: pkg.Active,
		}
		p.HandleSync(context.Background(), j)
		assert.Equal(t, p.capabilities, publish)
	})

}
