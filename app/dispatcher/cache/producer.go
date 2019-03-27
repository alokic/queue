package cache

import (
	"context"
	"github.com/alokic/queue/app/dispatcher"
)

// ProducerRepo struct.
type ProducerRepo struct {
	Map *Map
}

//NewProducerRepo is constructor for ProducerRepo.
func NewProducerRepo() dispatcher.ProducerRepository {
	return &ProducerRepo{
		Map: NewMap(),
	}
}

//Put creates or updates.
func (cr *ProducerRepo) Put(ctx context.Context, k string, producer dispatcher.Producer) {
	cr.Map.Set(k, producer)
}

//Get gets a producer.
func (cr *ProducerRepo) Get(ctx context.Context, k string) dispatcher.Producer {
	v := cr.Map.Get(k)
	if v == nil {
		return nil
	}

	p, ok := v.(dispatcher.Producer)
	if !ok {
		return nil
	}
	return p
}

//All lists all producers.
func (cr *ProducerRepo) All(ctx context.Context) []dispatcher.Producer {
	values := cr.Map.All(ctx)

	producers := []dispatcher.Producer{}

	for _, val := range values {
		con, ok := val.(dispatcher.Producer)
		if ok {
			producers = append(producers, con)
		}
	}
	return producers
}

//Delete deletes a producer.
func (cr *ProducerRepo) Delete(ctx context.Context, k string) {
	cr.Map.Delete(k)
}
