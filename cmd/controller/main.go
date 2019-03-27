package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	bg "github.com/bugsnag/bugsnag-go"
	kitstatsd "github.com/go-kit/kit/metrics/dogstatsd"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	"github.com/alokic/queue/app/controller/db"
	"github.com/alokic/queue/app/controller/dispatcher"
	"github.com/alokic/queue/app/controller/job"
	"github.com/alokic/queue/app/controller/system"
	v2config "github.com/honestbee/gopkg/config/v2"
	"github.com/honestbee/gopkg/httputils"
	log "github.com/honestbee/gopkg/logger"
	"github.com/honestbee/gopkg/sql"
	"github.com/honestbee/gopkg/stringutils"
	"github.com/newrelic/go-agent"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"runtime"
)

type printfLogger struct {
	logger log.Logger
}

// SvcConfig defines the configuration variables for this service
type SvcConfig struct {
	DBURL                       string `json:"db_url" usage:"DB url" required:"true"`
	Env                         string `json:"env" usage:"environment of service"`
	LogLevel                    string `json:"log_level" usage:"App Log Level"`
	NewrelicLicense             string `json:"newrelic_license" usage:"newrelic license"`
	BugsnagKey                  string `json:"bugsnag_key" usage:"bugsnag key"`
	DatadogFlushIntervalSeconds int    `json:"datadog_flush_interval_seconds" usage:"datadog flush interval in seconds"`
	HTTPPort                    int    `json:"http_port" usage:"http port number"`
	DBMaxOpenConns              int    `json:"db_open_conns" usage:"max open connections in database"`
	DBMaxIdleConns              int    `json:"max_db_idle_conns" usage:"max open connections in database"`
	DBConnectRetryCount         int    `json:"db_connect_retry_count" usage:"number of times to retry to connect to database"`
	ServerDrainTime             int    `json:"server_drain_time" usage:"number of seconds needed for server to shutdown"`
	Debug                       bool   `json:"debug" usage:"enable pprof to debug"`
}

// config defaults
var config = &SvcConfig{
	Env:                 "development",
	DBMaxOpenConns:      10,
	DBMaxIdleConns:      2,
	HTTPPort:            4000,
	DBConnectRetryCount: 5,
	ServerDrainTime:     5,
}

const (
	serverName      = "controller"
	defaultLogLevel = logrus.InfoLevel
	cleanUpTime     = time.Second
)

var (
	projectRoot      = fmt.Sprintf("%s/src/github.com/alokic/queue", os.Getenv("GOPATH"))
	webServer        *http.Server
	logger           log.Logger
	controllerCancel context.CancelFunc
	controllerDone   chan struct{}
	newrelicApm      newrelic.Application
	bgConfig         bg.Configuration
	statsd           *kitstatsd.Dogstatsd
)

// Build vars, dont change these
var (
	// GITCOMMIT means git commit tag
	GITCOMMIT = ""
	// GITBRANCH means git branch for the build
	GITBRANCH = ""
	// VERSION means release version
	VERSION = "0.0.0"
)

func (p *printfLogger) Printf(format string, v ...interface{}) {
	p.logger.Log(fmt.Sprintf(format, v...), "")
	debug.PrintStack()
}

// String representation of config
func (p *SvcConfig) String() string {
	d, _ := json.Marshal(p)
	return string(d)
}

func initializeConfig() {
	c := v2config.NewConfig(serverName, fmt.Sprintf("%s/config/%s/%s", projectRoot, serverName, config.Env))
	err := c.Load(config)
	if err != nil {
		fmt.Printf("config initialise error: %v", err)
		os.Exit(1)
	}
}

func initializeLogger() {
	level, err := logrus.ParseLevel(config.LogLevel)
	if err != nil {
		level = defaultLogLevel
	}
	config.LogLevel = fmt.Sprintf("%v", level)
	fmt.Printf("Setting log level to: %v\n", level)
	log.DefLogger = log.NewLogrus(level, os.Stdout, logrus.Fields{})
	logger = log.DefLogger.ContextualLogger(map[string]interface{}{"context": "main"})
}

func initNewrelic() {
	if config.NewrelicLicense != "" {
		app, err := newrelic.NewApplication(newrelic.NewConfig(serverName, config.NewrelicLicense))
		if nil != err {
			logger.Errorf("newrelic initialize error: %v", err)
			os.Exit(1)
		}
		newrelicApm = app
	}
}

func initBugsnag() {
	bgConfig = bg.Configuration{
		APIKey:          config.BugsnagKey,
		ProjectPackages: []string{"main", fmt.Sprintf("github.com/alokic/queue/%s", serverName)},
		ReleaseStage:    config.Env,
		Logger: &printfLogger{
			logger: log.DefLogger.ContextualLogger(map[string]interface{}{"context": "bugsnag"})},
	}
}

func initPostgreDB() *sql.DB {
	var d *sql.DB
	var err error

	for count := 0; count < config.DBConnectRetryCount; count++ {
		d, err = sql.NewDB("postgres", config.DBURL)

		if err == nil {
			err = d.Ping()
		}
		if err != nil {
			logger.Warnf("postgres connect error: %v (%v/%v)", err, count, config.DBConnectRetryCount)
			time.Sleep(2 * time.Second)
			continue
		}
		// this converts all struct  name to snakecase name to map to db column
		d.MapperFunc(func(s string) string {
			return stringutils.ToLowerSnakeCase(s)
		})

		d.SetMaxIdleConns(config.DBMaxIdleConns)
		d.SetMaxOpenConns(config.DBMaxOpenConns)
		return d
	}
	logger.Errorf("postgres connect error: %v. Exiting", err)
	os.Exit(1)
	return nil
}

func makeSytemHandler(d db.JobDB) http.Handler {
	service := system.New(
		d,
		logger,
	)

	service = system.NewLoggingService(
		logger,
		service,
	)

	return system.MakeSystemHandler(service, httputils.CreateRouter("Gorilla"), logger)
}

func makeJobHandler(d db.JobDB) http.Handler {
	fieldKeys := []string{"method"}

	service := job.New(
		d,
		logger,
	)

	service = job.NewLoggingService(
		logger,
		service,
	)

	service = job.NewInstrumentingService(
		kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: "api",
			Subsystem: "controller_jobconfig_service",
			Name:      "request_count",
			Help:      "Number of requests received.",
		}, fieldKeys),
		kitprometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: "api",
			Subsystem: "controller_jobconfig_service",
			Name:      "request_latency_microseconds",
			Help:      "Total duration of requests in microseconds.",
		}, fieldKeys),
		service,
	)

	return job.MakeJobHandler(service, httputils.CreateRouter("Gorilla"), logger)
}

func makeDispatcherHandler(dispatcherDB db.DispatcherDB, jobDB db.JobDB) http.Handler {
	fieldKeys := []string{"method"}

	service := dispatcher.NewDispatcherService(
		dispatcherDB,
		jobDB,
		logger,
	)

	service = dispatcher.NewLoggingService(
		logger,
		service,
	)

	service = dispatcher.NewInstrumentingService(
		kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: "api",
			Subsystem: "controller_dispatcher_service",
			Name:      "request_count",
			Help:      "Number of requests received.",
		}, fieldKeys),
		kitprometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: "api",
			Subsystem: "controller_dispatcher_service",
			Name:      "request_latency_microseconds",
			Help:      "Total duration of requests in microseconds.",
		}, fieldKeys),
		service,
	)

	return dispatcher.MakeDispatcherHandler(service, httputils.CreateRouter("Gorilla"), logger)
}

func recoverHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if r := recover(); r != nil {
				bg.Notify(fmt.Errorf("%v", r), bgConfig)
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte{})
			}
		}()
		h.ServeHTTP(w, r)
	})
}

func accessControl(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type")

		if r.Method == "OPTIONS" {
			return
		}

		h.ServeHTTP(w, r)
	})
}

func wrapHandlers(h http.Handler) http.Handler {
	return recoverHandler(accessControl(h))
}
func ppanic(st []byte) {
	for _, s := range strings.Split(string(st), "\n") {
		if len(s) == 0 {
			continue
		}
		if s[0:1] == "\t" {
			logger.Log("stack", fmt.Sprintf("    %s", s[1:]))
		}
	}
	logger.Log("exiting", "...")
	os.Exit(1)
}

func server() http.Handler {
	mux := http.NewServeMux()
	postgresDB := initPostgreDB()
	jobDB := db.NewJobDB(postgresDB, logger)
	dispatcherDB := db.NewDispatcherDB(postgresDB, logger)

	mux.Handle("/api/v1/job/", wrapHandlers(makeJobHandler(jobDB)))
	mux.Handle("/api/v1/dispatcher/", wrapHandlers(makeDispatcherHandler(dispatcherDB, jobDB)))
	mux.Handle("/health", wrapHandlers(makeSytemHandler(jobDB)))

	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func startHTTPServer() {
	defer func() {
		if r := recover(); r != nil {
			logger.Log("panic", r)
			ppanic(debug.Stack())
		}
	}()

	mux := server()
	webServer = &http.Server{Addr: ":" + fmt.Sprint(config.HTTPPort), Handler: mux}

	go func() {
		logger.Infof("transport: http, addr: %v, git-commit: %v, git-branch: %v, version: %v", config.HTTPPort, GITCOMMIT, GITBRANCH, VERSION)
		logger.Info(config)
		webServer.ListenAndServe()
	}()
}

func setupController() {
	controllerDone = make(chan struct{}, 1)
	_, cancel := context.WithCancel(context.Background())
	controllerCancel = cancel

	startHTTPServer()
}

func waitForever() {
	logger.Info("Entering wait loop")
	termChan := make(chan os.Signal, 2)
	dumpChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(dumpChan, syscall.SIGQUIT)
	select {
	case <-controllerDone:
		logger.Info("Exiting as dispatcher shutdown")
	case <-dumpChan:
		buf := make([]byte, 1<<16)
		runtime.Stack(buf, true)
		fmt.Printf("Dumping stack trace: \n%s", buf)
	case s := <-termChan:
		logger.Infof("received signal: %v. Exiting", s)
	}
}

func cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), cleanUpTime)
	webServer.Shutdown(ctx)
	time.Sleep(cleanUpTime)
	defer cancel()
	controllerCancel()
	select {
	case <-controllerDone:
	case <-time.After(cleanUpTime):
	}
}

func main() {
	initializeConfig()
	initializeLogger()
	initNewrelic()
	initBugsnag()
	setupController()
	waitForever()
	cleanup()
}
