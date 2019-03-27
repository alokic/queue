package main

import (
	"context"
	"fmt"
	"github.com/alokic/queue/examples/echoproducer"
	v2config "github.com/honestbee/gopkg/config/v2"
	log "github.com/honestbee/gopkg/logger"
	"github.com/sirupsen/logrus"
	"os"
)

/*
	This file demonstrates an example worker which produces to a kafka queue.
*/

const (
	appName         = "echoproducer"
	defaultLogLevel = logrus.InfoLevel
)

// SvcConfig defines the configuration variables for this service
type SvcConfig struct {
	DispatcherURL string `json:"dispatcher_url" usage:"Dispatcher url" required:"true"`
	LogLevel      string `json:"log_level" usage:"App Log Level"`
	ProducerJob   string `json:"producer_job" usage:"producer job"`
	NumMsgs       int    `json:"num_msgs" usage:"messages to publish"`
}

// config defaults
var config = &SvcConfig{
	DispatcherURL: "http://localhost:4001",
	LogLevel:      "info",
}

var (
	logger    log.Logger
	conCancel context.CancelFunc
)

func initializeConfig() {
	cfgPath := fmt.Sprintf("%s/src/github.com/alokic/queue/examples/echoproducer", os.Getenv("GOPATH"))
	fmt.Println(cfgPath)
	c := v2config.NewConfig(appName, cfgPath)
	err := c.Load(config)
	if err != nil {
		fmt.Printf("config initialise error: %v", err)
		os.Exit(1)
	}
	fmt.Println(config)
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

func setupWorker() {
	logger.Infof("setting up worker: %v", config.ProducerJob)
	ctx, cancel := context.WithCancel(context.Background())
	conCancel = cancel

	p := echoprodcuer.NewProducer(ctx, config.ProducerJob, config.DispatcherURL, config.NumMsgs, logger)
	if err := p.Setup(); err != nil {
		logger.Errorf("error in setting producer: %v", err)
		os.Exit(1)
	}
	p.Run()
}

//func waitForever() {
//	logger.Info("Entering wait loop")
//	termChan := make(chan os.Signal, 2)
//	dumpChan := make(chan os.Signal, 1)
//	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
//	signal.Notify(dumpChan, syscall.SIGQUIT)
//	select {
//	case sig := <-termChan:
//		logger.Infof("Received signal: %v", sig)
//	case <-dumpChan:
//		buf := make([]byte, 1<<16)
//		runtime.Stack(buf, true)
//		fmt.Printf("Dumping stack trace: \n%s", buf)
//	}
//	conCancel()
//	time.Sleep(5 * time.Second)
//	fmt.Println("Main done")
//}

func main() {
	initializeConfig()
	initializeLogger()
	setupWorker()
}
