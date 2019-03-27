// Package cache is cache for application entities.
package cache

import (
	"context"
	"github.com/alokic/queue/app/dispatcher"
)

//ConsumerRepo struct.
type ConsumerRepo struct {
	Map *Map
}

//NewConsumerRepo is constructor for ConsumerRepo.
func NewConsumerRepo() dispatcher.ConsumerRepository {
	return &ConsumerRepo{
		Map: NewMap(),
	}
}

//Put into repo.
func (cr *ConsumerRepo) Put(ctx context.Context, k string, consumer dispatcher.Consumer) {
	cr.Map.Set(k, consumer)
}

//Get from repo.
func (cr *ConsumerRepo) Get(ctx context.Context, k string) dispatcher.Consumer {
	v := cr.Map.Get(k)
	if v == nil {
		return nil
	}

	con, ok := v.(dispatcher.Consumer)
	if !ok {
		return nil
	}
	return con
}

//All entries in repo.
func (cr *ConsumerRepo) All(ctx context.Context) []dispatcher.Consumer {
	values := cr.Map.All(ctx)

	consumers := []dispatcher.Consumer{}

	for _, val := range values {
		con, ok := val.(dispatcher.Consumer)
		if ok {
			consumers = append(consumers, con)
		}
	}
	return consumers
}

//Delete from repo.
func (cr *ConsumerRepo) Delete(ctx context.Context, k string) {
	cr.Map.Delete(k)
}
