package harbour

import (
	"context"
	"github.com/alokic/queue/pkg"
	"github.com/honestbee/gopkg/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func Test_prepareRequest(t *testing.T) {
	/*
	  1. err in marshalling req body.
	  2. no body works.
	  3. err in http request
	*/

	logger.DefLogger = logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})
	tests := []struct {
		name      string
		method    string
		path      string
		body      interface{}
		shouldErr bool
	}{
		{name: "bad body errs", method: "POST", path: "/api", body: make(chan int), shouldErr: true},
		{name: "bad body errs", method: "GET", path: "/api", body: nil, shouldErr: false},
		{name: "bad http request errs", method: "?", path: "/api", body: nil, shouldErr: true},
	}

	for _, test := range tests {
		t.Log("Running: ", test.name)
		cl := New("alpha", logger.DefLogger).(*client)
		req, err := cl.prepareRequest(test.method, test.path, test.body)
		t.Log(err, req)
		if test.shouldErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

	}
}

func Test_do(t *testing.T) {
	/*
	   1. err in Do.
	   2. bad status.
	   3. no body gives 204.
	   4. all good.
	   5. timeouts.
	*/
	logger.DefLogger = logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})
	tests := []struct {
		name        string
		handler     http.HandlerFunc
		urlOverride string
		status      int
		body        []byte
		shouldErr   bool
	}{
		{
			name: "err in Do",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(500)
			},
			urlOverride: "foo",
			shouldErr:   true,
			status:      500,
			body:        []byte{},
		},
		{
			name: "connection refused",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(500)
			},
			urlOverride: "http://127.0.0.1:49950",
			shouldErr:   true,
			status:      500,
			body:        []byte{},
		},
		{
			name: "500 response",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(500)
			},
			shouldErr: false,
			status:    500,
			body:      []byte{},
		},
		{
			name: "no body gives 200 OK",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
			},
			shouldErr: false,
			status:    200,
			body:      []byte{},
		},
		{
			name: "body gives 200 OK",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte(`alpha`))
			},
			shouldErr: false,
			status:    200,
			body:      []byte(`alpha`),
		},
		{
			name: "body gives 200 OK",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte(`alpha`))
			},
			shouldErr: false,
			status:    200,
			body:      []byte(`alpha`),
		},
		{
			name: "timeout",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				time.Sleep(time.Second)
				writer.WriteHeader(200)
				writer.Write([]byte(`alpha`))
			},
			shouldErr: true,
			status:    200,
			body:      []byte(`alpha`),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	for _, test := range tests {
		t.Log("Running: ", test.name)
		cl := New("alpha", logger.DefLogger).(*client)
		handler := http.Handler(test.handler)
		s := httptest.NewServer(handler)
		defer s.Close()
		url := s.URL
		if test.urlOverride != "" {
			url = test.urlOverride
		}
		req, err := http.NewRequest("POST", url, nil)
		status, body, err := cl.do(ctx, req)
		t.Log(err, req)
		if test.shouldErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, test.status, status)
			assert.Equal(t, test.body, body)
		}
	}
}

func TestClient_SubscribeJob(t *testing.T) {
	/*
		1. error in doing request.
		3. bad code
		4. bad body
	*/
	tests := []struct {
		name            string
		host            string
		handler         http.HandlerFunc
		expectErr       bool
		expectErrSubStr string
	}{
		{
			name:            "error in Doing request",
			host:            "foo", //bad host name
			expectErr:       true,
			expectErrSubStr: "error in doing request",
		},
		{
			name: "bad return code",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "bad response code",
		},
		{
			name: "timeout",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				time.Sleep(time.Second)
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "context deadline exceeded",
		},
		{
			name: "no body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
			},
			expectErr:       true,
			expectErrSubStr: "end of JSON",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "404 returns ErrJobNotFound",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(404)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "job not found",
		},
		{
			name: "all good",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("{}"))
			},
			expectErr: false,
		},
	}

	for _, test := range tests {
		t.Log("Running: ", test.name)
		s := httptest.NewServer(test.handler)
		host := s.URL
		if test.host != "" {
			host = test.host
		}
		cl := New(host, logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})).(*client)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		rep, err := cl.SubscribeJob(ctx, nil)
		if test.expectErr {
			t.Log(err)
			assert.Error(t, err, "error expected in return")
			assert.True(t, strings.Contains(err.Error(), test.expectErrSubStr))
		} else {
			assert.NoError(t, err)
			assert.NotNil(t, rep)
		}
		s.Close()
		cancel()
	}
}

func TestClient_PublishJob(t *testing.T) {
	/*
		1. error in doing request.
		3. bad code
		4. bad body
	*/
	tests := []struct {
		name            string
		host            string
		handler         http.HandlerFunc
		expectErr       bool
		expectErrSubStr string
	}{
		{
			name:            "error in Doing request",
			host:            "foo", //bad host name
			expectErr:       true,
			expectErrSubStr: "error in doing request",
		},
		{
			name: "bad return code",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "bad response code",
		},
		{
			name: "timeout",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				time.Sleep(time.Second)
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "context deadline exceeded",
		},
		{
			name: "no body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
			},
			expectErr:       true,
			expectErrSubStr: "end of JSON",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "404 returns ErrJobNotFound",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(404)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "job not found",
		},
		{
			name: "all good",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("{}"))
			},
			expectErr: false,
		},
	}

	for _, test := range tests {
		t.Log("Running: ", test.name)
		s := httptest.NewServer(test.handler)
		host := s.URL
		if test.host != "" {
			host = test.host
		}
		cl := New(host, logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})).(*client)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		rep, err := cl.PublishJob(ctx, nil)
		if test.expectErr {
			t.Log(err)
			assert.Error(t, err, "error expected in return")
			assert.True(t, strings.Contains(err.Error(), test.expectErrSubStr))
		} else {
			assert.NoError(t, err)
			assert.NotNil(t, rep)
		}
		s.Close()
		cancel()
	}
}

func TestClient_UnsubscribeJob(t *testing.T) {
	/*
		1. error in doing request.
		3. bad code
		4. bad body
	*/
	tests := []struct {
		name            string
		host            string
		handler         http.HandlerFunc
		expectErr       bool
		expectErrSubStr string
	}{
		{
			name:            "error in Doing request",
			host:            "foo", //bad host name
			expectErr:       true,
			expectErrSubStr: "error in doing request",
		},
		{
			name: "bad return code",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "bad response code",
		},
		{
			name: "timeout",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				time.Sleep(time.Second)
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "context deadline exceeded",
		},
		{
			name: "no body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
			},
			expectErr:       true,
			expectErrSubStr: "end of JSON",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "404 returns ErrJobNotFound",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(404)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "job not found",
		},
		{
			name: "all good",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("{}"))
			},
			expectErr: false,
		},
	}

	for _, test := range tests {
		t.Log("Running: ", test.name)
		s := httptest.NewServer(test.handler)
		host := s.URL
		if test.host != "" {
			host = test.host
		}
		cl := New(host, logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})).(*client)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		rep, err := cl.UnsubscribeJob(ctx, nil)
		if test.expectErr {
			t.Log(err)
			assert.Error(t, err, "error expected in return")
			assert.True(t, strings.Contains(err.Error(), test.expectErrSubStr))
		} else {
			assert.NoError(t, err)
			assert.NotNil(t, rep)
		}
		s.Close()
		cancel()
	}
}

func TestClient_GetMsgs(t *testing.T) {
	/*
		1. error in doing request.
		3. bad code
		4. bad body
	*/
	tests := []struct {
		name            string
		host            string
		handler         http.HandlerFunc
		expectErr       bool
		expectErrSubStr string
		okResponse      *WorkerGetMsgsReply
	}{
		{
			name:            "error in Doing request",
			host:            "foo", //bad host name
			expectErr:       true,
			expectErrSubStr: "error in doing request",
		},
		{
			name: "bad return code",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "bad response code",
		},
		{
			name: "timeout",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				time.Sleep(time.Second)
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "context deadline exceeded",
		},
		{
			name: "no body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
			},
			expectErr:       true,
			expectErrSubStr: "end of JSON",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "404 returns ErrJobNotFound",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(404)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "job not found",
		},
		{
			name: "503 returns ErrServiceUnavailable",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(503)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "service unavailable",
		},
		{
			name: "all good",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte(`{"msgs":[{"id": 123}, {"id": 456, "name": "blah"}]}`))
			},
			expectErr: false,
			okResponse: &WorkerGetMsgsReply{
				Msgs: []pkg.Message{{ID: 123}, {ID: 456, Name: "blah"}},
			},
		},
	}

	for _, test := range tests {
		t.Log("Running: ", test.name)
		s := httptest.NewServer(test.handler)
		host := s.URL
		if test.host != "" {
			host = test.host
		}
		cl := New(host, logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})).(*client)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		rep, err := cl.GetMsgs(ctx, nil)
		if test.expectErr {
			t.Log(err)
			assert.Error(t, err, "error expected in return")
			assert.True(t, strings.Contains(err.Error(), test.expectErrSubStr))
		} else {
			assert.NoError(t, err)
			assert.NotNil(t, rep)
			assert.True(t, reflect.DeepEqual(rep.Msgs, test.okResponse.Msgs))
		}
		s.Close()
		cancel()
	}
}

func TestClient_GetRetryMsgs(t *testing.T) {
	/*
		1. error in doing request.
		3. bad code
		4. bad body
	*/
	tests := []struct {
		name            string
		host            string
		handler         http.HandlerFunc
		expectErr       bool
		expectErrSubStr string
		okResponse      *WorkerGetRetryMsgsReply
	}{
		{
			name:            "error in Doing request",
			host:            "foo", //bad host name
			expectErr:       true,
			expectErrSubStr: "error in doing request",
		},
		{
			name: "bad return code",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "bad response code",
		},
		{
			name: "timeout",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				time.Sleep(time.Second)
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "context deadline exceeded",
		},
		{
			name: "no body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
			},
			expectErr:       true,
			expectErrSubStr: "end of JSON",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "404 returns ErrJobNotFound",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(404)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "job not found",
		},
		{
			name: "503 returns ErrServiceUnavailable",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(503)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "service unavailable",
		},
		{
			name: "all good",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte(`{"msgs":[{"id": 123}, {"id": 456, "name": "blah"}]}`))
			},
			expectErr: false,
			okResponse: &WorkerGetRetryMsgsReply{
				Msgs: []pkg.Message{{ID: 123}, {ID: 456, Name: "blah"}},
			},
		},
	}

	for _, test := range tests {
		t.Log("Running: ", test.name)
		s := httptest.NewServer(test.handler)
		host := s.URL
		if test.host != "" {
			host = test.host
		}
		cl := New(host, logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})).(*client)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		rep, err := cl.GetRetryMsgs(ctx, nil)
		if test.expectErr {
			t.Log(err)
			assert.Error(t, err, "error expected in return")
			assert.True(t, strings.Contains(err.Error(), test.expectErrSubStr))
		} else {
			assert.NoError(t, err)
			assert.NotNil(t, rep)
			assert.True(t, reflect.DeepEqual(rep.Msgs, test.okResponse.Msgs))
		}
		s.Close()
		cancel()
	}
}

func TestClient_GetDLQMsgs(t *testing.T) {
	/*
		1. error in doing request.
		3. bad code
		4. bad body
	*/
	tests := []struct {
		name            string
		host            string
		handler         http.HandlerFunc
		expectErr       bool
		expectErrSubStr string
		okResponse      *WorkerGetDLQMsgsReply
	}{
		{
			name:            "error in Doing request",
			host:            "foo", //bad host name
			expectErr:       true,
			expectErrSubStr: "error in doing request",
		},
		{
			name: "bad return code",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "bad response code",
		},
		{
			name: "timeout",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				time.Sleep(time.Second)
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "context deadline exceeded",
		},
		{
			name: "no body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
			},
			expectErr:       true,
			expectErrSubStr: "end of JSON",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "404 returns ErrJobNotFound",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(404)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "job not found",
		},
		{
			name: "503 returns ErrServiceUnavailable",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(503)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "service unavailable",
		},
		{
			name: "all good",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte(`{"msgs": [{"reason": "alpha"}, {"reason": "beta"}]}`))
			},
			expectErr: false,
			okResponse: &WorkerGetDLQMsgsReply{
				Msgs: []pkg.DLQMessage{{Reason: "alpha"}, {Reason: "beta"}},
			},
		},
	}

	for _, test := range tests {
		t.Log("Running: ", test.name)
		s := httptest.NewServer(test.handler)
		host := s.URL
		if test.host != "" {
			host = test.host
		}
		cl := New(host, logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})).(*client)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		rep, err := cl.GetDLQMsgs(ctx, nil)
		if test.expectErr {
			t.Log(err)
			assert.Error(t, err, "error expected in return")
			assert.True(t, strings.Contains(err.Error(), test.expectErrSubStr))
		} else {
			assert.NoError(t, err)
			assert.NotNil(t, rep)
			assert.True(t, reflect.DeepEqual(rep.Msgs, test.okResponse.Msgs))
		}
		s.Close()
		cancel()
	}
}

func TestClient_AckMsgs(t *testing.T) {
	/*
		1. error in doing request.
		3. bad code
		4. bad body
	*/
	tests := []struct {
		name            string
		host            string
		handler         http.HandlerFunc
		expectErr       bool
		expectErrSubStr string
		okResponse      *WorkerAckMsgsReply
	}{
		{
			name:            "error in Doing request",
			host:            "foo", //bad host name
			expectErr:       true,
			expectErrSubStr: "error in doing request",
		},
		{
			name: "bad return code",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "bad response code",
		},
		{
			name: "timeout",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				time.Sleep(time.Second)
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "context deadline exceeded",
		},
		{
			name: "no body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
			},
			expectErr:       true,
			expectErrSubStr: "end of JSON",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "404 returns ErrJobNotFound",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(404)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "job not found",
		},
		{
			name: "503 returns ErrServiceUnavailable",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(503)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "service unavailable",
		},
		{
			name: "all good",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte(`{"failed_ids": [1,3], "bad_ids": [2], "error": "wow"}`))
			},
			expectErr: false,
			okResponse: &WorkerAckMsgsReply{
				FailedIDs: []uint64{1, 3},
				BadIDs:    []uint64{2},
				Error:     "wow",
			},
		},
	}

	for _, test := range tests {
		t.Log("Running: ", test.name)
		s := httptest.NewServer(test.handler)
		host := s.URL
		if test.host != "" {
			host = test.host
		}
		cl := New(host, logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})).(*client)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		rep, err := cl.AckMsgs(ctx, nil)
		if test.expectErr {
			t.Log(err)
			assert.Error(t, err, "error expected in return")
			assert.True(t, strings.Contains(err.Error(), test.expectErrSubStr))
		} else {
			assert.NoError(t, err)
			assert.NotNil(t, rep)
			assert.True(t, reflect.DeepEqual(rep, test.okResponse))
		}
		s.Close()
		cancel()
	}
}

func TestClient_NackMsgs(t *testing.T) {
	/*
		1. error in doing request.
		3. bad code
		4. bad body
	*/
	tests := []struct {
		name            string
		host            string
		handler         http.HandlerFunc
		expectErr       bool
		expectErrSubStr string
		okResponse      *WorkerNackMsgsReply
	}{
		{
			name:            "error in Doing request",
			host:            "foo", //bad host name
			expectErr:       true,
			expectErrSubStr: "error in doing request",
		},
		{
			name: "bad return code",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "bad response code",
		},
		{
			name: "timeout",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				time.Sleep(time.Second)
				writer.WriteHeader(500)
			},
			expectErr:       true,
			expectErrSubStr: "context deadline exceeded",
		},
		{
			name: "no body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
			},
			expectErr:       true,
			expectErrSubStr: "end of JSON",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "bad body",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "failed to unmarshall",
		},
		{
			name: "404 returns ErrJobNotFound",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(404)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "job not found",
		},
		{
			name: "503 returns ErrServiceUnavailable",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(503)
				writer.Write([]byte("wow"))
			},
			expectErr:       true,
			expectErrSubStr: "service unavailable",
		},
		{
			name: "all good",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(200)
				writer.Write([]byte(`{"failed_ids": [1,3], "bad_ids": [2], "error": "wow"}`))
			},
			expectErr: false,
			okResponse: &WorkerNackMsgsReply{
				FailedIDs: []uint64{1, 3},
				BadIDs:    []uint64{2},
				Error:     "wow",
			},
		},
	}

	for _, test := range tests {
		t.Log("Running: ", test.name)
		s := httptest.NewServer(test.handler)
		host := s.URL
		if test.host != "" {
			host = test.host
		}
		cl := New(host, logger.NewLogrus(logrus.DebugLevel, os.Stderr, map[string]interface{}{})).(*client)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		rep, err := cl.NackMsgs(ctx, nil)
		if test.expectErr {
			t.Log(err)
			assert.Error(t, err, "error expected in return")
			assert.True(t, strings.Contains(err.Error(), test.expectErrSubStr))
		} else {
			assert.NoError(t, err)
			assert.NotNil(t, rep)
			assert.True(t, reflect.DeepEqual(rep, test.okResponse))
		}
		s.Close()
		cancel()
	}
}
