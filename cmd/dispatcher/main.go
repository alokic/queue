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
	"github.com/alokic/queue/app/dispatcher"
	"github.com/alokic/queue/app/dispatcher/cache"
	"github.com/alokic/queue/app/dispatcher/remote"
	"github.com/alokic/queue/app/dispatcher/system"
	"github.com/alokic/queue/app/dispatcher/worker"
	"github.com/alokic/queue/pkg"
	v2config "github.com/honestbee/gopkg/config/v2"
	"github.com/honestbee/gopkg/httputils"
	log "github.com/honestbee/gopkg/logger"
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
	Env                         string `json:"env" usage:"environment of service" required:"true" `
	MasterURL                   string `json:"controller_url" usage:"controller url" required:"true"`
	NativeQueueAddress          string `json:"native_queue_address" usage:"Native Queue address" required:"true"`
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
	ControllerSyncInterval      int    `json:"controller_sync_interval" usage:"how often to sync from controller"`
}

// config defaults
var config = &SvcConfig{
	Env:                    "production",
	DBMaxOpenConns:         10,
	DBMaxIdleConns:         2,
	HTTPPort:               4001,
	DBConnectRetryCount:    5,
	ServerDrainTime:        5,
	ControllerSyncInterval: 10000,
}

const (
	serverName      = "dispatcher"
	defaultLogLevel = logrus.InfoLevel
	cleanUpTime     = 2 * time.Second
)

var (
	projectRoot      = fmt.Sprintf("%s/src/github.com/alokic/queue", os.Getenv("GOPATH"))
	manager          dispatcher.WorkerService
	webServer        *http.Server
	logger           log.Logger
	dispatcherCancel context.CancelFunc
	dispatcherDone   chan struct{}
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
	fmt.Printf("ENV: %v\n", config.Env)
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
		ProjectPackages: []string{"main", fmt.Sprintf("github.com/honestbee/%s", serverName)},
		ReleaseStage:    config.Env,
		Logger: &printfLogger{
			logger: log.DefLogger.ContextualLogger(map[string]interface{}{"context": "bugsnag"})},
	}
}

func makeSytemHandler() http.Handler {
	service := system.New(
		logger,
	)

	service = system.NewLoggingService(
		logger,
		service,
	)

	return system.MakeSystemHandler(service, httputils.CreateRouter("Gorilla"), logger)
}

func makeWorkerHandler(service dispatcher.WorkerService) http.Handler {
	fieldKeys := []string{"method"}

	service = worker.NewLoggingService(service, logger)

	service = worker.NewInstrumentingService(
		kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: "api",
			Subsystem: "dispatcher_worker_service",
			Name:      "request_count",
			Help:      "Number of requests received.",
		}, fieldKeys),
		kitprometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: "api",
			Subsystem: "dispatcher_worker_service",
			Name:      "request_latency_microseconds",
			Help:      "Total duration of requests in microseconds.",
		}, fieldKeys),
		service,
	)

	return worker.MakeWorkerHandler(service, httputils.CreateRouter("Gorilla"), logger)
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
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
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

func server(s dispatcher.WorkerService) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/api/v1/worker/", wrapHandlers(makeWorkerHandler(s)))
	mux.Handle("/health", wrapHandlers(makeSytemHandler()))

	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func startHTTPServer(s dispatcher.WorkerService) {
	defer func() {
		if r := recover(); r != nil {
			logger.Log("panic", r)
			ppanic(debug.Stack())
		}
	}()

	mux := server(s)
	webServer = &http.Server{Addr: ":" + fmt.Sprint(config.HTTPPort), Handler: mux}

	go func() {
		logger.Infof("transport: http, addr: %v, git-commit: %v, git-branch: %v, version: %v", config.HTTPPort, GITCOMMIT, GITBRANCH, VERSION)
		logger.Info(config)
		webServer.ListenAndServe()
	}()
}

func setupDispatcher() {
	controllerStub := remote.NewController(config.MasterURL, logger)
	consumerRepo := cache.NewConsumerRepo()
	producerRepo := cache.NewProducerRepo()
	jobRepo := cache.NewJobRepo()
	id := pkg.MustGid(5)

	manager = worker.NewService(id, controllerStub, jobRepo, consumerRepo, producerRepo, config.NativeQueueAddress, time.Duration(config.ControllerSyncInterval)*time.Millisecond, logger)
	manager.Init()

	startHTTPServer(manager)
}

func waitForever() {
	logger.Info("Entering wait loop")
	termChan := make(chan os.Signal, 2)
	dumpChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(dumpChan, syscall.SIGQUIT)
	select {
	case <-dumpChan:
		buf := make([]byte, 1<<16)
		runtime.Stack(buf, true)
		fmt.Printf("Dumping stack trace: \n%s", buf)
	case s := <-termChan:
		logger.Infof("received signal: %v. Exiting", s)
	}
}

func cleanup() {
	manager.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), cleanUpTime)
	webServer.Shutdown(ctx)
	defer cancel()

	select {
	case <-time.After(cleanUpTime):
	}
}

func main() {
	initializeConfig()
	initializeLogger()
	initNewrelic()
	initBugsnag()
	setupDispatcher()
	waitForever()
	cleanup()
}
