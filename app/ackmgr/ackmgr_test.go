package ackmgr

import (
	"github.com/alokic/queue/app/dispatcher/queue"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestAckMgr_Init(t *testing.T) {
	if logger.DefLogger == nil {
		logger.DefLogger = logger.NewLogrus(logrus.DebugLevel, os.Stdout, map[string]interface{}{})
	}

	t.Run("all good", func(t *testing.T) {
		ackP := &queue.MockProducer{}
		ackP.Init()

		am := New("test", ackP, logger.DefLogger)
		assert.NoError(t, am.Init())
		assert.False(t, am.failedInit)
	})

	t.Run("am init fails", func(t *testing.T) {
		ackP := &queue.MockProducer{FailInit: true}
		ackP.Init()

		am := New("test", ackP, logger.DefLogger)
		assert.Error(t, am.Init())
		assert.True(t, am.failedInit)
	})
}

func TestAckMgr_Push(t *testing.T) {
	if logger.DefLogger == nil {
		logger.DefLogger = logger.NewLogrus(logrus.DebugLevel, os.Stdout, map[string]interface{}{})
	}

	t.Run("push on uninited AckMgr", func(t *testing.T) {
		ackP := &queue.MockProducer{}
		am := New("test", ackP, logger.DefLogger)

		msg := pkg.AckMessage{}
		assert.True(t, am.failedInit)
		assert.Error(t, am.Push(msg))
	})

	t.Run("push to failed AckMgr", func(t *testing.T) {
		ackP := &queue.MockProducer{FailInit: true}

		am := New("test", ackP, logger.DefLogger)
		am.Init()
		msg := pkg.AckMessage{}

		assert.True(t, am.failedInit)
		assert.Error(t, am.Push(msg))
	})

	t.Run("push OK", func(t *testing.T) {
		ackP := &queue.MockProducer{FailInit: false}

		am := New("test", ackP, logger.DefLogger)
		assert.NoError(t, am.Init())
		msg := pkg.AckMessage{}

		assert.NoError(t, am.Push(msg))
		select {
		case <-ackP.PublishStream:
		case <-time.After(time.Second):
			t.Fatal("didnt get publish ack from AckM Q")
		}
	})
}
