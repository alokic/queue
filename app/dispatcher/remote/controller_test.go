package remote

import (
	"encoding/json"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestController_Register(t *testing.T) {
	/*
		1. controller down.
		2. controller timeout.
		3. controller 404.
		4. controller 500.
		5. controller OK with bad body.
		6. all good.
	*/
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})

	tests := map[string]struct {
		expectedJobs   int
		expectedErr    bool
		expectedErrStr string
		mockedHandler  http.HandlerFunc
	}{
		"controller timeout": {
			expectedJobs:   0,
			expectedErr:    true,
			expectedErrStr: "context deadline exceeded",
			mockedHandler: func(writer http.ResponseWriter, request *http.Request) {
				time.Sleep(2 * time.Second)
				writer.WriteHeader(http.StatusInternalServerError)
				responseBody := DispatcherJobRegisterReply{}
				response, _ := json.Marshal(responseBody)
				writer.Write(response)
			},
		},
		"controller 404": {
			expectedJobs:   0,
			expectedErr:    true,
			expectedErrStr: "job not found",
			mockedHandler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusNotFound)
				responseBody := DispatcherJobRegisterReply{}
				response, _ := json.Marshal(responseBody)
				writer.Write(response)
			},
		},
		"controller 500": {
			expectedJobs:   0,
			expectedErr:    true,
			expectedErrStr: "bad status code",
			mockedHandler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusInternalServerError)
				responseBody := DispatcherJobRegisterReply{}
				response, _ := json.Marshal(responseBody)
				writer.Write(response)
			},
		},
		"controller bad reponse body": {
			expectedJobs:   0,
			expectedErr:    true,
			expectedErrStr: "",
			mockedHandler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusOK)
				responseBody := "alpha"
				response, _ := json.Marshal(responseBody)
				writer.Write(response)
			},
		},
		"all good": {
			expectedJobs:   1,
			expectedErr:    false,
			expectedErrStr: "",
			mockedHandler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusOK)
				responseBody := DispatcherJobRegisterReply{
					Job: &pkg.Job{},
				}
				response, _ := json.Marshal(responseBody)
				writer.Write(response)
			},
		},
	}

	for name, test := range tests {
		t.Log("Running: ", name)
		testHandler := http.HandlerFunc(test.mockedHandler)
		testControllerServer := httptest.NewServer(testHandler)
		contr := NewController(testControllerServer.URL, logger)
		reqTimeout = time.Second
		job, err := contr.Register(1, "alpha")
		if test.expectedErr {
			assert.Error(t, err)
			assert.Contains(t, err.Error(), test.expectedErrStr)
			t.Log(err)
		} else {
			assert.NoError(t, err)
			assert.NotNil(t, job)
		}
		testControllerServer.Close()
	}
}

func TestController_Deregister(t *testing.T) {
	/*
		2. controller timeout.
		3. controller 404.
		4. controller 500.
		5. controller OK with bad body.
		6. all good.
	*/
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})

	tests := map[string]struct {
		expectedErr    bool
		expectedErrStr string
		mockedHandler  http.HandlerFunc
	}{
		"controller timeout": {
			expectedErr:    true,
			expectedErrStr: "context deadline exceeded",
			mockedHandler: func(writer http.ResponseWriter, request *http.Request) {
				time.Sleep(2 * time.Second)
				writer.WriteHeader(http.StatusInternalServerError)
			},
		},
		"controller 500": {
			expectedErr:    true,
			expectedErrStr: "bad status code",
			mockedHandler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusInternalServerError)
				responseBody := DispatcherJobDeregisterReply{}
				response, _ := json.Marshal(responseBody)
				writer.Write(response)
			},
		},
		"controller bad response body": {
			expectedErr:    true,
			expectedErrStr: "",
			mockedHandler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusOK)
				responseBody := "alpha"
				response, _ := json.Marshal(responseBody)
				writer.Write(response)
			},
		},
		"all good": {
			expectedErr:    false,
			expectedErrStr: "",
			mockedHandler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusOK)
				responseBody := DispatcherJobDeregisterReply{}
				response, _ := json.Marshal(responseBody)
				writer.Write(response)
			},
		},
	}

	for name, test := range tests {
		t.Log("Running: ", name)
		testHandler := http.HandlerFunc(test.mockedHandler)
		testControllerServer := httptest.NewServer(testHandler)
		contr := NewController(testControllerServer.URL, logger)
		reqTimeout = time.Second
		err := contr.Deregister(1, 1)
		if test.expectedErr {
			assert.Error(t, err)
			assert.Contains(t, err.Error(), test.expectedErrStr)
			t.Log(err)
		} else {
			assert.NoError(t, err)
		}
		testControllerServer.Close()
	}
}

func TestController_Gossip(t *testing.T) {
	/*
		2. controller timeout.
		3. controller 404.
		4. controller 500.
		5. controller OK with bad body.
		6. all good.
	*/
	logger := logger.NewLogrus(logrus.InfoLevel, os.Stdout, map[string]interface{}{})

	tests := map[string]struct {
		expectedJobs   int
		expectedErr    bool
		expectedErrStr string
		mockedHandler  http.HandlerFunc
	}{
		"controller timeout": {
			expectedJobs:   0,
			expectedErr:    true,
			expectedErrStr: "context deadline exceeded",
			mockedHandler: func(writer http.ResponseWriter, request *http.Request) {
				time.Sleep(2 * time.Second)
				writer.WriteHeader(http.StatusInternalServerError)
			},
		},
		"controller 500": {
			expectedJobs:   0,
			expectedErr:    true,
			expectedErrStr: "bad status code",
			mockedHandler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusInternalServerError)
				responseBody := DispatcherJobDeregisterReply{}
				response, _ := json.Marshal(responseBody)
				writer.Write(response)
			},
		},
		"controller bad response body": {
			expectedJobs:   0,
			expectedErr:    true,
			expectedErrStr: "",
			mockedHandler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusOK)
				responseBody := "alpha"
				response, _ := json.Marshal(responseBody)
				writer.Write(response)
			},
		},
		"all good": {
			expectedJobs:   2,
			expectedErr:    false,
			expectedErrStr: "",
			mockedHandler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusOK)
				responseBody := DispatcherGossipReply{
					Jobs: []*pkg.Job{&pkg.Job{}, &pkg.Job{}},
				}
				response, _ := json.Marshal(responseBody)
				writer.Write(response)
			},
		},
	}

	for name, test := range tests {
		t.Log("Running: ", name)
		testHandler := http.HandlerFunc(test.mockedHandler)
		testControllerServer := httptest.NewServer(testHandler)
		contr := NewController(testControllerServer.URL, logger)
		reqTimeout = time.Second
		jobs, err := contr.Gossip(1)
		if test.expectedErr {
			assert.Error(t, err)
			assert.Contains(t, err.Error(), test.expectedErrStr)
			t.Log(err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, test.expectedJobs, len(jobs))
		}
		testControllerServer.Close()
	}
}
