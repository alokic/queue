package db

import (
	"context"
	sql2 "database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/honestbee/gopkg/sql"
)

type (
	//JobRepo struct.
	JobRepo struct {
		jobDB     *sql.DB
		fieldInfo *sql.FieldInfo
		logger    logger.Logger
	}

	jobAttrib struct {
		ID                uint64 `db:"id"`
		Name              string `db:"name"`
		Description       string `db:"description"`
		Type              string `db:"type"`
		State             string `db:"state"`
		QueueConfig       string `db:"queue_config"`
		RetryConfig       string `db:"retry_config"`
		MaxProcessingTime int    `db:"max_processing_time"`
		Version           int    `db:"version"`
	}
)

var (
	getActiveJobQuery         = "SELECT id, name, description, type, state, queue_config, retry_config, max_processing_time, version from job where name = $1 and state in ('active', 'stopped')"
	getManyJobsQuery          = "SELECT id, name, description, type, state, queue_config, retry_config, max_processing_time, version from job where id in (%s)"
	consumerConfigInsertQuery = "INSERT into job(%s) VALUES(%s)"
	consumerConfigUpdateQuery = "UPDATE job SET %s where %s"
	healthCheckQuery          = "SELECT WHERE 1=0"
)

//NewJobDB constructor.
func NewJobDB(jobDB *sql.DB, log logger.Logger) JobDB {
	c := map[string]interface{}{"context": "jobdb"}
	lg := log.ContextualLogger(c)
	return &JobRepo{jobDB: jobDB, fieldInfo: sql.GenFieldInfo(jobAttrib{}), logger: lg}
}

//Get for getting a ConsumerWorklet.
func (jr *JobRepo) Get(ctx context.Context, name string) (*pkg.Job, error) {
	var ja jobAttrib
	err := jr.jobDB.QueryRowx(getActiveJobQuery, name).StructScan(&ja)
	if err != nil {
		if err == sql2.ErrNoRows {
			return nil, pkg.ErrNotFound{Err: fmt.Errorf("no job found")}
		}
		return nil, errors.Wrapf(err, "error while getting job for: %v", name)
	}
	j := ja.toJob()
	if j == nil {
		return nil, errors.New(fmt.Sprintf("error in getting job from jobAttrib: %v", ja))
	}
	return j, nil
}

//GetMany for getting all jobs.
func (jr *JobRepo) GetMany(ctx context.Context, ids []uint64) ([]*pkg.Job, error) {
	idList := []string{}
	for _, id := range ids {
		idList = append(idList, strconv.FormatUint(id, 10))
	}
	idStr := strings.Join(idList, ",")

	rows, err := jr.jobDB.Queryx(fmt.Sprintf(getManyJobsQuery, idStr))
	if err != nil {
		return nil, errors.Wrap(err, "error while getting jobs")
	}

	jobs := []*pkg.Job{}
	for rows.Next() {
		var ja jobAttrib
		err = rows.StructScan(&ja)
		if err != nil {
			jr.logger.Errorf("error in scanning to jobAttrib: %v", err)
			return nil, err
		}
		j := ja.toJob()
		if j == nil {
			return nil, errors.New(fmt.Sprintf("error in getting job from jobAttrib: %v", ja))
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

//Create for creating consumer job.
func (jr *JobRepo) Create(ctx context.Context, in *pkg.Job) error {
	tx := jr.jobDB.Begin()
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	if err := jr.create(ctx, tx, toJobAtrribs(in)); err != nil {
		tx.Rollback()
		return errors.Wrap(err, "error in creating job")
	}
	return tx.Commit()
}

func (jr *JobRepo) create(ctx context.Context, tx *sql.Tx, ja *jobAttrib) error {
	stmnt := fmt.Sprintf(
		consumerConfigInsertQuery,
		" id, name, description, type, state, queue_config, retry_config, max_processing_time, version, created_at, updated_at",
		fmt.Sprintf(" '%d', '%s', '%s', '%s', '%s', '%s', '%s', '%d', '%d', '%s', '%s'",
			ja.ID, ja.Name, ja.Description, ja.Type, ja.State, ja.QueueConfig, ja.RetryConfig, ja.MaxProcessingTime, ja.Version, time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339)))

	_, err := tx.Exec(stmnt)
	return err
}

//Update for updating existing job in-place.
func (jr *JobRepo) Update(ctx context.Context, newJob *pkg.Job) error {
	oldJob, err := jr.Get(ctx, newJob.Name)
	if err != nil {
		return errors.Wrap(err, "error in getting old consumer job")
	}

	mergedJob := pkg.MergeJob(*oldJob, *newJob)
	ja := toJobAtrribs(&mergedJob)
	if ja == nil {
		return errors.New(fmt.Sprintf("error in converting job to jobatrrib: %v", mergedJob))
	}

	stmnt := fmt.Sprintf(
		consumerConfigUpdateQuery,
		fmt.Sprintf(" name = '%s', state = '%s', type = '%s', queue_config = '%s',retry_config = '%s', max_processing_time = '%d', version = '%d', updated_at = '%s'",
			ja.Name, ja.State, ja.Type, ja.QueueConfig, ja.RetryConfig, ja.MaxProcessingTime, ja.Version, time.Now().UTC().Format(time.RFC3339)),
		fmt.Sprintf(" id = '%d'", oldJob.ID))
	if _, err = jr.jobDB.Exec(stmnt); err != nil {
		return errors.Wrap(err, "error in updating job")
	}

	return nil
}

//ArchivingUpdate deprecates old version and creates a new version.
func (jr *JobRepo) ArchivingUpdate(ctx context.Context, newJob *pkg.Job) error {
	oldJob, err := jr.Get(ctx, newJob.Name)
	if err != nil {
		return errors.Wrap(err, "error in getting old job")
	}

	oldJobAttrib := toJobAtrribs(oldJob)
	newJobAttrib := toJobAtrribs(newJob)
	newJobAttrib.Version = oldJobAttrib.Version + 1
	mergedJobAttrib := mergeJobAttrib(*oldJobAttrib, *newJobAttrib)

	tx := jr.jobDB.Begin()
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	//deprecate old version.
	{
		_, err = tx.Exec(
			fmt.Sprintf(consumerConfigUpdateQuery, fmt.Sprintf(" state = 'deprecated', updated_at = '%s'", time.Now().UTC().Format(time.RFC3339)),
				fmt.Sprintf(" id = '%d'", oldJobAttrib.ID)))
		if err != nil {
			tx.Rollback()
			jr.logger.Errorf("error in deprecating job: %v", err)
			return errors.Wrap(err, "error in deprecating old job")
		}
	}

	//create new version.
	if err := jr.create(ctx, tx, &mergedJobAttrib); err != nil {
		tx.Rollback()
		jr.logger.Errorf("error in creating job: %v", err)
		return errors.Wrap(err, "error in updating job")
	}

	return tx.Commit()
}

//Archive marks as deleted.
func (jr *JobRepo) Archive(ctx context.Context, name string) error {
	job, err := jr.Get(ctx, name)
	if err != nil {
		return errors.Wrap(err, "error in getting job")
	}

	if err = jr.jobDB.ExecTxn(
		fmt.Sprintf(consumerConfigUpdateQuery, fmt.Sprintf(" state = 'archived', updated_at = '%s'", time.Now().UTC().Format(time.RFC3339)),
			fmt.Sprintf(" id = '%d'", job.ID))); err != nil {
		jr.logger.Errorf("error in archiving job: %v", err)
		return errors.Wrap(err, "error in archiving job")
	}
	return nil
}

//HealthCheck checks for jobDB up.
func (jr *JobRepo) HealthCheck(ctx context.Context) error {
	if oc := jr.jobDB.DB.Stats().OpenConnections; oc >= 100 {
		jr.logger.Warnf("HIGH open connections: %v", oc)
	}

	r, err := jr.jobDB.Query(healthCheckQuery)
	if err != nil {
		jr.logger.Errorf("postgres healthcheck error: %v", err)
		return errors.Wrap(err, "error in postgres")
	}
	defer r.Close() //always close all rows OR iterate fully so as to return connection to pool

	return nil
}

func (ja *jobAttrib) toJob() *pkg.Job {
	if ja == nil {
		return nil
	}

	j := new(pkg.Job)
	j.ID = ja.ID
	j.Name = ja.Name
	j.Description = ja.Description
	j.Type = pkg.JobType(ja.Type)
	j.State = pkg.JobState(ja.State)
	j.MaxProcessingTime = ja.MaxProcessingTime
	j.Version = ja.Version

	//if jobattrib's QueueConfig is "", then job's QueueConfig is nil.
	if ja.QueueConfig != "" {
		qc := new(pkg.QueueConfig)
		if err := json.Unmarshal([]byte(ja.QueueConfig), qc); err != nil {
			return nil
		}
		j.QueueConfig = qc
	}

	//if jobattrib's RetryConfig is "", then job's RetryConfig is nil.
	if ja.RetryConfig != "" {
		rc := new(pkg.RetryConfig)
		if err := json.Unmarshal([]byte(ja.RetryConfig), rc); err != nil {
			return nil
		}
		j.RetryConfig = rc
	}

	return j
}

func toJobAtrribs(j *pkg.Job) *jobAttrib {
	if j == nil {
		return nil
	}

	ja := new(jobAttrib)
	ja.ID = j.ID
	ja.Name = j.Name
	ja.Description = j.Description
	ja.Type = string(j.Type)
	ja.State = string(j.State)
	ja.Version = j.Version
	ja.MaxProcessingTime = j.MaxProcessingTime

	//if job's QueueConfig is nil, then we store empty string in DB.
	if j.QueueConfig != nil {
		b, err := json.Marshal(j.QueueConfig)
		if err != nil {
			fmt.Printf("ERROR: error while marshalling queue_config: %v", err)
			return nil
		}
		ja.QueueConfig = string(b)
	}

	//if job's RetryConfig is nil, then we store empty string in DB.
	if j.RetryConfig != nil {
		b, err := json.Marshal(j.RetryConfig)
		if err != nil {
			fmt.Printf("ERROR: error while marshalling retry_config: %v", err)
			return nil
		}
		ja.RetryConfig = string(b)
	}

	return ja
}

//mergeJobAttrib merges b into a.
func mergeJobAttrib(a, b jobAttrib) jobAttrib {
	if b.ID != 0 {
		a.ID = b.ID
	}

	if b.Name != "" {
		a.Name = b.Name
	}

	if b.Description != "" {
		a.Description = b.Description
	}

	if b.Type != "" {
		a.Type = b.Type
	}

	if b.State != "" {
		a.State = b.State
	}

	if b.QueueConfig != "" {
		a.QueueConfig = b.QueueConfig
	}

	if b.RetryConfig != "" {
		a.RetryConfig = b.RetryConfig
	}

	if b.MaxProcessingTime != 0 {
		a.MaxProcessingTime = b.MaxProcessingTime
	}

	if b.Version != 0 {
		a.Version = b.Version
	}

	return a
}
