package dispatcher

import (
	"context"

	"github.com/alokic/queue/pkg"
)

type (
	//Producer interface for Producer worklet.
	Producer interface {
		Init() error
		Publish([]*pkg.Message) error
		HandleSync(context.Context, *pkg.Job)
		Close() error
	}
)

// ProducerRepository is storage interface for producer.
type ProducerRepository interface {
	Put(context.Context, string, Producer)
	Get(context.Context, string) Producer
	All(context.Context) []Producer
	Delete(context.Context, string)
}
