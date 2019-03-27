package worker

import (
	"errors"
	"fmt"
	"github.com/alokic/queue/pkg"
	"sync"
)

//MockController struct.
type MockController struct {
	Job       *pkg.Job
	GossipErr bool
	Mu        sync.RWMutex
}

//Gossip method for getting status.
func (mc *MockController) Gossip(dispatcherID uint64) ([]*pkg.Job, error) {
	mc.Mu.RLock()
	defer mc.Mu.RUnlock()

	if mc.GossipErr {
		return nil, errors.New("bad")
	}

	return []*pkg.Job{mc.Job}, nil
}

//Register for registering to controller.
func (mc *MockController) Register(dispatcherID uint64, jobname string) (*pkg.Job, error) {
	mc.Mu.RLock()
	defer mc.Mu.RUnlock()

	if mc.GossipErr {
		return nil, errors.New("bad")
	}

	if jobname != mc.Job.Name {
		return nil, errors.New("bad job")
	}

	fmt.Printf("Register reply: %v\n", *mc.Job.QueueConfig)

	return mc.Job, nil
}

//Deregister from controller.
func (mc *MockController) Deregister(dispatcherID uint64, jobid uint64) error {
	mc.Mu.RLock()
	defer mc.Mu.RUnlock()

	if mc.GossipErr {
		return errors.New("bad")
	}

	return nil
}
