package db

import (
	"context"
	"github.com/alokic/queue/app/controller"
	"github.com/honestbee/gopkg/logger"
	"github.com/honestbee/gopkg/sql"
	"github.com/pkg/errors"
	"time"
)

type (
	//DispatcherRepo struct.
	DispatcherRepo struct {
		dispatcherDB *sql.DB
		fieldInfo    *sql.FieldInfo
		logger       logger.Logger
	}

	dispatcherAttrib struct {
		ID           uint64    `db:"id"`
		DispatcherID uint64    `db:"dispatcher_id"`
		JobID        uint64    `db:"job_id"`
		RegisteredAt time.Time `db:"registered_at"`
		HeartbeatAt  time.Time `db:"heartbeat_at"`
	}
)

var (
	allDispatcherQuery    = "SELECT id, dispatcher_id, job_id, registered_at, heartbeat_at FROM dispatcher"
	allJobsQuery          = "SELECT id, dispatcher_id, job_id, registered_at, heartbeat_at FROM dispatcher where dispatcher_id = $1"
	deleteJobQuery        = "DELETE FROM dispatcher where dispatcher_id = $1 AND job_id = $2"
	deleteDispatcherQuery = "DELETE FROM dispatcher where dispatcher_id = $1"
)

//NewDispatcherDB constructor.
func NewDispatcherDB(dispatcherDB *sql.DB, log logger.Logger) DispatcherDB {
	c := map[string]interface{}{"context": "jobdb"}
	lg := log.ContextualLogger(c)
	return &DispatcherRepo{dispatcherDB: dispatcherDB, fieldInfo: sql.GenFieldInfo(dispatcherAttrib{}), logger: lg}
}

//GetJobs gets all jobs for a dispatcher.
func (drepo *DispatcherRepo) GetJobs(ctx context.Context, dispatcherID uint64) ([]*controller.DispatcherJob, error) {
	rows, err := drepo.dispatcherDB.Queryx(allJobsQuery, dispatcherID)
	if err != nil {
		return nil, errors.Wrap(err, "error in getting jobs")
	}

	das := []*dispatcherAttrib{}
	for rows.Next() {
		da := new(dispatcherAttrib)
		if err := rows.StructScan(&da); err != nil {
			return nil, errors.Wrap(err, "error in scanning row")
		}
		das = append(das, da)
	}

	return toDispatcherJobs(das), nil
}

//CreateJob creates a new dispatcher job entry.
func (drepo *DispatcherRepo) CreateJob(ctx context.Context, dj *controller.DispatcherJob) error {
	if dj == nil {
		return errors.New("dispatcher job is nil")
	}

	da := fromDispatcherJob(dj)
	da.RegisteredAt = time.Now().UTC()
	da.HeartbeatAt = time.Now().UTC()

	return drepo.dispatcherDB.BatchInsert("dispatcher", []interface{}{da}, drepo.fieldInfo)
}

//DeleteJob creates a new dispatcher job entry.
func (drepo *DispatcherRepo) DeleteJob(ctx context.Context, j *controller.DispatcherJob) error {
	if _, err := drepo.dispatcherDB.Exec(deleteJobQuery, j.DispatcherID, j.JobID); err != nil {
		return errors.Wrap(err, "error in deleting job")
	}
	return nil
}

//DeleteDispatcher deletes a dispatcher job entry.
func (drepo *DispatcherRepo) DeleteDispatcher(ctx context.Context, dj uint64) error {
	if _, err := drepo.dispatcherDB.Exec(deleteDispatcherQuery, dj); err != nil {
		return errors.Wrap(err, "error in deleting dispatcher")
	}
	return nil
}

func toDispatcherJobs(das []*dispatcherAttrib) []*controller.DispatcherJob {
	jobs := []*controller.DispatcherJob{}

	for _, d := range das {
		if dj := toDispatcherJob(d); dj != nil {
			jobs = append(jobs, dj)
		}
	}

	return jobs
}

func toDispatcherJob(da *dispatcherAttrib) *controller.DispatcherJob {
	if da == nil {
		return nil
	}

	dj := new(controller.DispatcherJob)
	dj.ID = da.ID
	dj.DispatcherID = da.DispatcherID
	dj.JobID = da.JobID

	return dj
}

func fromDispatcherJob(dj *controller.DispatcherJob) *dispatcherAttrib {
	if dj == nil {
		return nil
	}

	da := new(dispatcherAttrib)
	da.ID = dj.ID
	da.DispatcherID = dj.DispatcherID
	da.JobID = dj.JobID

	return da
}
