//Package dispatcher is home for dispatcher.
package dispatcher

import (
	"context"
	"github.com/alokic/queue/pkg"
)

// JobRepository is storage interface for job.
type JobRepository interface {
	Put(context.Context, string, *pkg.Job)
	Get(context.Context, string) *pkg.Job
	Delete(context.Context, string)
}
