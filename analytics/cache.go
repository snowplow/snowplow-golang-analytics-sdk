//
// Copyright (c) 2021 Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Apache License Version 2.0,
// and you may not use this file except in compliance with the Apache License Version 2.0.
// You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the Apache License Version 2.0 is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
//

package analytics

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// SchemaCache is the interface for schema lookup caching.
type SchemaCache interface {
	get(key string) (string, bool)
	put(key, value string)
}

// SetSchemaCache replaces the cache implementation used by fixSchema.
func SetSchemaCache(c SchemaCache) {
	schemaCache = c
}

// --- sync.Map cache ---

// schemaMapCache is a bounded cache using sync.Map.
// Reads are lock-free. When maxSize is reached, the entire cache is purged.
type schemaMapCache struct {
	m       sync.Map
	size    atomic.Int64
	maxSize int64
}

// NewSchemaMapCache creates a sync.Map-based cache that purges when full.
func NewSchemaMapCache(maxSize int) *schemaMapCache {
	return &schemaMapCache{maxSize: int64(maxSize)}
}

func (c *schemaMapCache) get(key string) (string, bool) {
	if c.maxSize == 0 {
		return "", false
	}
	v, ok := c.m.Load(key)
	if ok {
		return v.(string), true
	}
	return "", false
}

func (c *schemaMapCache) put(key, value string) {
	if c.maxSize <= 0 {
		return
	}
	if c.size.Load() >= c.maxSize {
		c.m.Clear()
		c.size.Store(0)
	}
	if _, loaded := c.m.LoadOrStore(key, value); !loaded {
		c.size.Add(1)
	}
}

// --- LRU cache ---

type lruEntry struct {
	key   string
	value string
}

// LRUCache is a thread-safe LRU cache with bounded size.
// Uses a map for O(1) lookup and a doubly-linked list for O(1) LRU tracking.
type LRUCache struct {
	maxSize int
	cache   map[string]*list.Element
	lruList *list.List
	mu      sync.Mutex
}

// NewLRUCache creates an LRU cache that evicts the least recently used entry when full.
func NewLRUCache(maxSize int) *LRUCache {
	return &LRUCache{
		maxSize: maxSize,
		cache:   make(map[string]*list.Element),
		lruList: list.New(),
	}
}

func (c *LRUCache) get(key string) (string, bool) {
	if c.maxSize == 0 {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.cache[key]; ok {
		c.lruList.MoveToFront(elem)
		return elem.Value.(*lruEntry).value, true
	}
	return "", false
}

func (c *LRUCache) put(key, value string) {
	if c.maxSize == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.cache[key]; ok {
		c.lruList.MoveToFront(elem)
		elem.Value.(*lruEntry).value = value
		return
	}
	if len(c.cache) >= c.maxSize {
		oldest := c.lruList.Back()
		if oldest != nil {
			c.lruList.Remove(oldest)
			delete(c.cache, oldest.Value.(*lruEntry).key)
		}
	}
	entry := &lruEntry{key: key, value: value}
	elem := c.lruList.PushFront(entry)
	c.cache[key] = elem
}

var schemaCache SchemaCache = NewSchemaMapCache(1000)
