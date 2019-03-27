package harbour

import (
	"context"
)

type (
	MockClient struct {
		Err               error
		NumErrs           int
		errCount          int //should be reset
		Subscribereply    *WorkerSubscribeJobReply
		Unsubscribereply  *WorkerUnsubscribeJobReply
		GetMsgsreply      *WorkerGetMsgsReply
		GetRetryMsgsreply *WorkerGetRetryMsgsReply
		GetDLQMsgsreply   *WorkerGetDLQMsgsReply
		AckMsgsreply      *WorkerAckMsgsReply
		NackMsgsreply     *WorkerNackMsgsReply
		Publishreply      *WorkerPublishJobReply
	}
)

func (c *MockClient) SubscribeJob(ctx context.Context, r *WorkerSubscribeJobRequest) (*WorkerSubscribeJobReply, error) {
	if c.errCount >= c.NumErrs {
		return c.Subscribereply, nil
	}
	c.errCount++
	return nil, c.Err
}

func (c *MockClient) UnsubscribeJob(ctx context.Context, r *WorkerUnsubscribeJobRequest) (*WorkerUnsubscribeJobReply, error) {
	if c.errCount >= c.NumErrs {
		return c.Unsubscribereply, nil
	}
	c.errCount++
	return nil, c.Err
}

func (c *MockClient) PublishJob(ctx context.Context, r *WorkerPublishJobRequest) (*WorkerPublishJobReply, error) {
	if c.errCount >= c.NumErrs {
		return c.Publishreply, nil
	}
	c.errCount++
	return nil, c.Err
}

func (c *MockClient) GetMsgs(ctx context.Context, r *WorkerGetMsgsRequest) (*WorkerGetMsgsReply, error) {
	if c.errCount >= c.NumErrs {
		return c.GetMsgsreply, nil
	}
	c.errCount++
	return nil, c.Err
}

func (c *MockClient) GetRetryMsgs(ctx context.Context, r *WorkerGetRetryMsgsRequest) (*WorkerGetRetryMsgsReply, error) {
	if c.errCount >= c.NumErrs {
		return c.GetRetryMsgsreply, nil
	}
	c.errCount++
	return nil, c.Err
}

func (c *MockClient) GetDLQMsgs(ctx context.Context, r *WorkerGetDLQMsgsRequest) (*WorkerGetDLQMsgsReply, error) {
	if c.errCount >= c.NumErrs {
		return c.GetDLQMsgsreply, nil
	}
	c.errCount++
	return nil, c.Err
}

func (c *MockClient) AckMsgs(ctx context.Context, r *WorkerAckMsgsRequest) (*WorkerAckMsgsReply, error) {
	if c.errCount >= c.NumErrs {
		return c.AckMsgsreply, nil
	}
	c.errCount++
	return nil, c.Err
}

func (c *MockClient) NackMsgs(ctx context.Context, r *WorkerNackMsgsRequest) (*WorkerNackMsgsReply, error) {
	if c.errCount >= c.NumErrs {
		return c.NackMsgsreply, nil
	}
	c.errCount++
	return nil, c.Err
}

func (c *MockClient) HealthCheck(ctx context.Context) (*WorkerHealthCheckReply, error) {
	return &WorkerHealthCheckReply{}, nil
}
