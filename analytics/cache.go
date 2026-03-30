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
	"sync"
	"sync/atomic"
)

// schemaMapCache is a bounded cache using sync.Map.
// Per sync.Map docs: "optimized for when the entry for a given key is only
// ever written once but read many times, as in caches that only grow."
// When maxSize is reached, the entire cache is purged.
type schemaMapCache struct {
	m       sync.Map
	size    atomic.Int64
	maxSize atomic.Int64
}

func newSchemaMapCache(maxSize int) *schemaMapCache {
	c := &schemaMapCache{}
	c.maxSize.Store(int64(maxSize))
	return c
}

func (c *schemaMapCache) get(key string) (string, bool) {
	if c.maxSize.Load() == 0 {
		return "", false
	}
	v, ok := c.m.Load(key)
	if ok {
		return v.(string), true
	}
	return "", false
}

func (c *schemaMapCache) put(key, value string) {
	maxSz := c.maxSize.Load()
	if maxSz <= 0 {
		return
	}
	if c.size.Load() >= maxSz {
		c.m.Clear()
		c.size.Store(0)
	}
	if _, loaded := c.m.LoadOrStore(key, value); !loaded {
		c.size.Add(1)
	}
}

var schemaCache = newSchemaMapCache(10000)

// SetSchemaCacheSize configures the maximum number of cached schemas.
// The default size is 10,000. Pass 0 to disable caching.
// The cache is cleared when the size changes.
// Intended to be called once at startup, before processing events.
func SetSchemaCacheSize(maxSize int) {
	schemaCache.m.Clear()
	schemaCache.size.Store(0)
	schemaCache.maxSize.Store(int64(maxSize))
}
