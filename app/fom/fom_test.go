package fom

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

func TestFOM_Init(t *testing.T) {
	if logger.DefLogger == nil {
		logger.DefLogger = logger.NewLogrus(logrus.DebugLevel, os.Stdout, map[string]interface{}{})
	}

	t.Run("all good", func(t *testing.T) {
		rp1 := &queue.MockProducer{}
		rp := []queue.Producer{rp1}

		dlqp := &queue.MockProducer{}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)
		assert.NoError(t, fom.Init())
		assert.False(t, fom.failedInit)
	})

	t.Run("dlq init fails", func(t *testing.T) {
		rp1 := &queue.MockProducer{}
		rp := []queue.Producer{rp1}

		dlqp := &queue.MockProducer{FailInit: true}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)
		assert.Error(t, fom.Init())
		assert.True(t, fom.failedInit)
	})

	t.Run("retry init fails", func(t *testing.T) {
		rp1 := &queue.MockProducer{FailInit: true}
		rp := []queue.Producer{rp1}

		dlqp := &queue.MockProducer{}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)
		assert.Error(t, fom.Init())
		assert.True(t, fom.failedInit)
	})
}

func TestFOM_Push(t *testing.T) {
	if logger.DefLogger == nil {
		logger.DefLogger = logger.NewLogrus(logrus.DebugLevel, os.Stdout, map[string]interface{}{})
	}

	t.Run("push on uninited FOM", func(t *testing.T) {
		rp1 := &queue.MockProducer{}
		rp := []queue.Producer{rp1}

		dlqp := &queue.MockProducer{}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)

		msg := pkg.Message{}
		assert.True(t, fom.failedInit)
		assert.Error(t, fom.Push(msg))
	})

	t.Run("push to DLQ on retry exhaust: #1", func(t *testing.T) {
		rp1 := &queue.MockProducer{}
		rp := []queue.Producer{rp1}

		dlqp := &queue.MockProducer{}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)
		fom.Init()

		msg := pkg.Message{Attempt: 1}
		assert.NoError(t, fom.Push(msg))
		select {
		case <-dlqp.PublishStream:
		case <-time.After(time.Second):
			t.Fatal("didnt get publish ack from DLQ")
		}
	})

	t.Run("push to DLQ on retry exhaust: #2", func(t *testing.T) {
		rp1 := &queue.MockProducer{}
		rp := []queue.Producer{rp1}

		dlqp := &queue.MockProducer{}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)
		fom.Init()

		msg := pkg.Message{Attempt: 4, ErrorPolicy: pkg.ErrorPolicy{RepeatCount: 3}}
		assert.NoError(t, fom.Push(msg))
		select {
		case <-dlqp.PublishStream:
		case <-time.After(time.Second):
			t.Fatal("didnt get publish ack from DLQ")
		}
	})

	t.Run("push to RQ on retry: #1", func(t *testing.T) {
		rp1 := &queue.MockProducer{}
		rp2 := &queue.MockProducer{}
		rp := []queue.Producer{rp1, rp2}

		dlqp := &queue.MockProducer{}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)
		fom.Init()

		msg := pkg.Message{Attempt: 1, ErrorPolicy: pkg.ErrorPolicy{RepeatCount: 3, RepeatQueue: -1}}
		assert.NoError(t, fom.Push(msg))
		select {
		case <-rp1.PublishStream:
		case <-time.After(time.Second):
			t.Fatal("didnt get publish ack from DLQ")
		}
	})

	t.Run("push to RQ on retry: #2", func(t *testing.T) {
		rp1 := &queue.MockProducer{}
		rp2 := &queue.MockProducer{}
		rp := []queue.Producer{rp1, rp2}

		dlqp := &queue.MockProducer{}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)
		fom.Init()

		msg := pkg.Message{Attempt: 2, ErrorPolicy: pkg.ErrorPolicy{RepeatCount: 3, RepeatQueue: -1}}
		assert.NoError(t, fom.Push(msg))
		select {
		case <-rp2.PublishStream:
		case <-time.After(time.Second):
			t.Fatal("didnt get publish ack from DLQ")
		}
	})

	t.Run("push to RQ on retry: #3", func(t *testing.T) {
		rp1 := &queue.MockProducer{}
		rp2 := &queue.MockProducer{}
		rp := []queue.Producer{rp1, rp2}

		dlqp := &queue.MockProducer{}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)
		fom.Init()

		msg := pkg.Message{Attempt: 3, ErrorPolicy: pkg.ErrorPolicy{RepeatCount: 3, RepeatQueue: -1}}
		assert.NoError(t, fom.Push(msg))
		select {
		case <-rp2.PublishStream:
		case <-time.After(time.Second):
			t.Fatal("didnt get publish ack from DLQ")
		}
	})

	t.Run("push to frozen RQ on retry", func(t *testing.T) {
		rp1 := &queue.MockProducer{}
		rp2 := &queue.MockProducer{}
		rp := []queue.Producer{rp1, rp2}

		dlqp := &queue.MockProducer{}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)
		fom.Init()

		msg := pkg.Message{Attempt: 1, ErrorPolicy: pkg.ErrorPolicy{RepeatCount: 3, RepeatQueue: 1}}
		assert.NoError(t, fom.Push(msg))
		select {
		case <-rp2.PublishStream:
		case <-time.After(time.Second):
			t.Fatal("didnt get publish ack from Q")
		}

		msg = pkg.Message{Attempt: 2, ErrorPolicy: pkg.ErrorPolicy{RepeatCount: 2, RepeatQueue: 1}}
		assert.NoError(t, fom.Push(msg))
		select {
		case <-rp2.PublishStream:
		case <-time.After(time.Second):
			t.Fatal("didnt get publish ack from Q")
		}

		msg = pkg.Message{Attempt: 3, ErrorPolicy: pkg.ErrorPolicy{RepeatCount: 2, RepeatQueue: 1}}
		assert.NoError(t, fom.Push(msg))
		select {
		case <-dlqp.PublishStream:
		case <-time.After(time.Second):
			t.Fatal("didnt get publish ack from Q")
		}

	})

	t.Run("push to bad frozen RQ on retry picks last", func(t *testing.T) {
		rp1 := &queue.MockProducer{}
		rp2 := &queue.MockProducer{}
		rp := []queue.Producer{rp1, rp2}

		dlqp := &queue.MockProducer{}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)
		fom.Init()

		msg := pkg.Message{Attempt: 1, ErrorPolicy: pkg.ErrorPolicy{RepeatCount: 3, RepeatQueue: 10}}
		assert.NoError(t, fom.Push(msg))
		select {
		case <-rp2.PublishStream:
		case <-time.After(time.Second):
			t.Fatal("didnt get publish ack from Q")
		}

		msg = pkg.Message{Attempt: 2, ErrorPolicy: pkg.ErrorPolicy{RepeatCount: 2, RepeatQueue: 1}}
		assert.NoError(t, fom.Push(msg))
		select {
		case <-rp2.PublishStream:
		case <-time.After(time.Second):
			t.Fatal("didnt get publish ack from Q")
		}

		msg = pkg.Message{Attempt: 3, ErrorPolicy: pkg.ErrorPolicy{RepeatCount: 2, RepeatQueue: 1}}
		assert.NoError(t, fom.Push(msg))
		select {
		case <-dlqp.PublishStream:
		case <-time.After(time.Second):
			t.Fatal("didnt get publish ack from Q")
		}
	})
}

func TestFOM_ToDLQ(t *testing.T) {
	if logger.DefLogger == nil {
		logger.DefLogger = logger.NewLogrus(logrus.DebugLevel, os.Stdout, map[string]interface{}{})
	}

	t.Run("push to uninited FOM", func(t *testing.T) {
		rp1 := &queue.MockProducer{}
		rp2 := &queue.MockProducer{}
		rp := []queue.Producer{rp1, rp2}

		dlqp := &queue.MockProducer{}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)
		assert.True(t, fom.failedInit)
		assert.Error(t, fom.ToDLQ([]byte("a"), "max"))
	})

	t.Run("push to failed FOM", func(t *testing.T) {
		rp1 := &queue.MockProducer{}
		rp2 := &queue.MockProducer{FailInit: true}
		rp := []queue.Producer{rp1, rp2}

		dlqp := &queue.MockProducer{}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)
		assert.Error(t, fom.Init())
		assert.True(t, fom.failedInit)
		assert.Error(t, fom.ToDLQ([]byte("a"), "max"))
	})

	t.Run("push OK", func(t *testing.T) {
		rp1 := &queue.MockProducer{}
		rp2 := &queue.MockProducer{}
		rp := []queue.Producer{rp1, rp2}

		dlqp := &queue.MockProducer{}

		fom := New("test", &pkg.RetryConfig{BackOffs: []uint{1}}, rp, dlqp, logger.DefLogger)
		assert.NoError(t, fom.Init())
		assert.NoError(t, fom.ToDLQ([]byte("a"), "max"))
		select {
		case <-dlqp.PublishStream:
		case <-time.After(time.Second):
			t.Fatal("didnt get publish ack from Q")
		}
	})
}
