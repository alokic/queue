package db

import (
	"context"
	"github.com/alokic/queue/app/controller"
	"github.com/alokic/queue/pkg"
)

type (

	//JobDB interface.
	JobDB interface {
		Get(context.Context, string) (*pkg.Job, error)
		GetMany(context.Context, []uint64) ([]*pkg.Job, error)
		Create(context.Context, *pkg.Job) error
		Update(context.Context, *pkg.Job) error
		ArchivingUpdate(context.Context, *pkg.Job) error
		Archive(context.Context, string) error
		HealthCheck(context.Context) error
	}

	//DispatcherDB interface
	DispatcherDB interface {
		GetJobs(context.Context, uint64) ([]*controller.DispatcherJob, error)
		CreateJob(context.Context, *controller.DispatcherJob) error
		DeleteJob(context.Context, *controller.DispatcherJob) error
		DeleteDispatcher(context.Context, uint64) error
	}
)
