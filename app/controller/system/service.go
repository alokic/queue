package system

import (
	"context"
	"github.com/alokic/queue/app/controller"
	"github.com/alokic/queue/app/controller/db"
	"github.com/honestbee/gopkg/logger"
	"github.com/pkg/errors"
)

type (
	systemService struct {
		db     db.JobDB
		logger logger.Logger
	}
)

//New constructor.
func New(db db.JobDB, logger logger.Logger) controller.SystemService {
	lg := logger.ContextualLogger(map[string]interface{}{"context": "system-service"})
	return &systemService{db: db, logger: lg}
}

func (ss *systemService) HealthCheck(ctx context.Context) (*controller.HealthCheckReply, error) {
	if err := ss.db.HealthCheck(ctx); err != nil {
		return nil, errors.Wrap(err, "unable to connect to DB")
	}
	rep := new(controller.HealthCheckReply)
	return rep, nil
}
