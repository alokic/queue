// Package cache is lame and local cache.
package cache

import (
	"context"
	"sync"
)

// Map implementation for concurrent use.
type Map struct {
	sync.RWMutex
	m map[string]interface{}
}

// NewMap is constarctor for cache.
func NewMap() *Map {
	return &Map{
		m: make(map[string]interface{}),
	}
}

// Get a key.
func (c *Map) Get(k string) interface{} {
	c.RLock()
	defer c.RUnlock()
	return c.m[k]
}

// All values.
func (c *Map) All(ctx context.Context) []interface{} {
	c.RLock()
	defer c.RUnlock()

	r := []interface{}{}
	for _, v := range c.m {
		r = append(r, v)
	}
	return r
}

// Set a kv.
func (c *Map) Set(k string, v interface{}) {
	c.Lock()
	defer c.Unlock()
	c.m[k] = v
}

// Delete a key.
func (c *Map) Delete(k string) {
	c.Lock()
	defer c.Unlock()
	delete(c.m, k)
}
