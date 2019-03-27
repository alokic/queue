package dispatcher

import (
	"context"

	"github.com/alokic/queue/app/dispatcher/queue"
	"github.com/alokic/queue/pkg"
)

type (
	//Consumer interface for consumer job.
	Consumer interface {
		Init(queue.Producer, []queue.Producer, queue.Producer) error
		GetMsgs(context.Context, uint) ([]pkg.Message, error)
		GetRetryMsgs(context.Context, uint, uint) ([]pkg.Message, error)
		GetDLQMsgs(context.Context, uint) ([]pkg.DLQMessage, error)
		Ack(context.Context, []uint64) ([]uint64, []uint64, error)  //keeps a map from job id to queue msg handle
		Nack(context.Context, []uint64) ([]uint64, []uint64, error) //keeps a map from job id to queue msg handle
		HandleSync(context.Context, *pkg.Job)
		Close() error
	}
)

// ConsumerRepository is storage interface for consumer
type ConsumerRepository interface {
	Put(context.Context, string, Consumer)
	Get(context.Context, string) Consumer
	All(context.Context) []Consumer
	Delete(context.Context, string)
}
