package db

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/honestbee/gopkg/sql"
	"github.com/honestbee/gopkg/stringutils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

/*
Assumes postgres to be running with postgres user and db "queue_test" created.
Also all migrations need to be run.
*/

var db *sql.DB
var log logger.Logger

func init() {
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		dbURL = "sslmode=disable dbname=queue_test"
	}
	fmt.Printf("DBURL: %v\n", dbURL)
	d, err := sql.NewDB("postgres", dbURL)

	if err == nil {
		err = d.Ping()
	}

	if err != nil {
		panic(fmt.Sprintf("error in setting up DB.Make sure postgres is running and user, database exists.:%v", err))
	}
	// this converts all struct  name to snakecase name to map to db column
	d.MapperFunc(func(s string) string {
		return stringutils.ToLowerSnakeCase(s)
	})

	db = d
	log = logger.NewLogrus(logrus.DebugLevel, os.Stdout, map[string]interface{}{})
}

/*
 Note that we dont test any validation here as thats done at service level.
*/

func strfromList(a []uint) string {
	res := []string{}
	for _, e := range a {
		res = append(res, strconv.FormatUint(uint64(e), 10))
	}
	return strings.Join(res, ",")
}

func areEqualJSON(s1, s2 string) (bool, error) {
	var o1 interface{}
	var o2 interface{}

	var err error
	err = json.Unmarshal([]byte(s1), &o1)
	if err != nil {
		return false, fmt.Errorf("Error mashalling string 1 :: %s", err.Error())
	}
	err = json.Unmarshal([]byte(s2), &o2)
	if err != nil {
		return false, fmt.Errorf("Error mashalling string 2 :: %s", err.Error())
	}

	return reflect.DeepEqual(o1, o2), nil
}

func equal(a, b pkg.Job) bool {
	if a.ID != b.ID || a.Name != b.Name || a.Type != b.Type || a.State != b.State ||
		a.Description != b.Description || a.Version != b.Version || a.MaxProcessingTime != b.MaxProcessingTime {
		return false
	}

	if a.QueueConfig == nil {
		if b.QueueConfig != nil {
			return false
		}
	} else {
		if b.QueueConfig == nil {
			return false
		}
		if a.QueueConfig.Type != b.QueueConfig.Type {
			return false
		}

		a1, _ := json.Marshal(a.QueueConfig.Config)
		b1, _ := json.Marshal(b.QueueConfig.Config)
		if ok, err := areEqualJSON(string(a1), string(b1)); !ok {
			fmt.Printf("err: %v\n", err)
			fmt.Printf("a: %v\n", string(a1))
			fmt.Printf("b: %v\n", string(b1))
			return false
		}
	}

	if a.RetryConfig == nil {
		if b.RetryConfig != nil {
			return false
		}
	} else {
		if b.RetryConfig == nil {
			return false
		}
		if strfromList(a.RetryConfig.BackOffs) != strfromList(b.RetryConfig.BackOffs) {
			return false
		}
	}
	return true
}

func TestJobRepo(t *testing.T) {
	jobsdb := NewJobDB(db, log)
	ctx := context.Background()

	//cleanup
	defer func() {
		rawJobs := jobsdb.(*JobRepo)
		rawJobs.jobDB.Exec("DELETE FROM job")
	}()

	//Try creating a job
	job := pkg.Job{
		ID:          1,
		Name:        "alpha",
		Description: "beta",
		Type:        pkg.ConsumerJob,
		State:       pkg.Stopped,
		QueueConfig: &pkg.QueueConfig{
			Type:   pkg.KafkaQueue,
			Config: pkg.KafkaConfig{ClusterAddress: []string{"alpha"}, Topic: "alpha"}},
		RetryConfig:       &pkg.RetryConfig{BackOffs: []uint{1}},
		MaxProcessingTime: 10,
		Version:           1,
	}

	_, err := jobsdb.Get(ctx, job.Name)
	assert.Error(t, err)

	assert.NoError(t, jobsdb.Create(ctx, &job))
	j, err := jobsdb.Get(ctx, job.Name)
	assert.NoError(t, err)
	assert.True(t, equal(job, *j)) //both job should match

	//Create another job
	job2 := job
	job2.ID = 2
	job2.Name = "gamma"
	assert.NoError(t, jobsdb.Create(ctx, &job2))
	j2, err := jobsdb.Get(ctx, job2.Name)
	assert.NoError(t, err)
	assert.True(t, equal(job2, *j2)) //both job should match

	//GetMany jobs
	jobs, err := jobsdb.GetMany(ctx, []uint64{1, 2, 3}) //non existing ids are ignored
	assert.NoError(t, err)
	assert.Equal(t, len(jobs), 2)
	assert.True(t, equal(*jobs[0], job))  //both job should match
	assert.True(t, equal(*jobs[1], job2)) //both job should match

	//Update a job
	updatedJob := job
	updatedJob.State = pkg.Active
	updatedJob.RetryConfig = &pkg.RetryConfig{BackOffs: []uint{1, 2}}
	assert.NoError(t, jobsdb.Update(ctx, &updatedJob))
	j, err = jobsdb.Get(ctx, updatedJob.Name)
	assert.NoError(t, err)
	assert.True(t, equal(updatedJob, *j)) //both job should match

	//ArchivingUpdate
	job3 := job
	job3.ID = 3
	job3.Name = "delta"
	job3.Version = 99
	job3.State = pkg.Active
	assert.NoError(t, jobsdb.Create(ctx, &job3))
	updatedJob3 := job3
	updatedJob3.ID = 4
	updatedJob3.QueueConfig = &pkg.QueueConfig{
		Type:   pkg.KafkaQueue,
		Config: pkg.KafkaConfig{ClusterAddress: []string{"delta"}, Topic: "delta"}}
	updatedJob.RetryConfig = &pkg.RetryConfig{BackOffs: []uint{3}}
	updatedJob3.Version = 100 //we manually set it to +1
	assert.NoError(t, jobsdb.ArchivingUpdate(ctx, &updatedJob3))
	j, err = jobsdb.Get(ctx, updatedJob3.Name)
	assert.NoError(t, err)
	assert.True(t, equal(updatedJob3, *j)) //both job should match
	rawJobs := jobsdb.(*JobRepo)

	var jobstate string
	rawJobs.jobDB.QueryRowx("Select state from job where id = 3").Scan(&jobstate)
	assert.Equal(t, "deprecated", jobstate)
	rawJobs.jobDB.QueryRowx("Select state from job where id = 4").Scan(&jobstate)
	assert.Equal(t, "active", jobstate)

	var jobVersion int
	rawJobs.jobDB.QueryRowx("Select version from job where id = 3").Scan(&jobVersion)
	assert.Equal(t, 99, jobVersion)
	rawJobs.jobDB.QueryRowx("Select version from job where id = 4").Scan(&jobVersion)
	assert.Equal(t, 100, jobVersion)

	//Archive
	assert.NoError(t, jobsdb.Archive(ctx, updatedJob3.Name))
	rawJobs.jobDB.QueryRowx("Select state from job where id = 4").Scan(&jobstate)
	assert.Equal(t, "archived", jobstate)
	j, err = jobsdb.Get(ctx, updatedJob3.Name)
	assert.Error(t, err) //error now
}
