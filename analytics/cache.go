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
	"fmt"
	"sync"
	"sync/atomic"
)

// CacheStats represents a snapshot of cache performance metrics.
// All counters are cumulative since cache initialization or last clear.
type CacheStats struct {
	Size      int     // Current number of cached entries
	Hits      uint64  // Total cache hits since initialization/clear
	Misses    uint64  // Total cache misses since initialization/clear
	Evictions uint64  // Total LRU evictions since initialization/clear
	HitRate   float64 // Calculated hit rate: hits/(hits+misses), range [0.0-1.0]
}

// cacheEntry stores the key-value pair for a cached schema transformation.
// The key is stored to enable reverse lookup during eviction.
type cacheEntry struct {
	key   string
	value string
}

// lruCache implements a thread-safe LRU (Least Recently Used) cache with bounded size.
// Uses a map for O(1) lookup and a doubly-linked list for O(1) LRU tracking.
type lruCache struct {
	maxSize   int                      // Maximum number of entries (0 = disabled)
	cache     map[string]*list.Element // O(1) lookup by key
	lruList   *list.List               // Doubly-linked list for LRU ordering
	mu        sync.RWMutex             // Thread-safety for concurrent access
	hits      atomic.Uint64            // Cache hit counter (lock-free)
	misses    atomic.Uint64            // Cache miss counter (lock-free)
	evictions atomic.Uint64            // Eviction counter (lock-free)
}

// newLRUCache creates a new LRU cache with the specified maximum size.
// maxSize=0 disables caching (all lookups miss).
// maxSize>0 enables caching with LRU eviction when limit reached.
func newLRUCache(maxSize int) *lruCache {
	return &lruCache{
		maxSize: maxSize,
		cache:   make(map[string]*list.Element),
		lruList: list.New(),
	}
}

// get retrieves a value from the cache by key.
// If found, moves the entry to front (most recently used) and returns (value, true).
// If not found, increments miss counter and returns ("", false).
// Thread-safe for concurrent access.
func (c *lruCache) get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[key]; ok {
		c.lruList.MoveToFront(elem)
		c.hits.Add(1)
		return elem.Value.(*cacheEntry).value, true
	}

	c.misses.Add(1)
	return "", false
}

// put adds or updates a key-value pair in the cache.
// If key exists, updates value and moves to front (most recently used).
// If key doesn't exist and cache is full, evicts LRU entry before adding.
// Thread-safe for concurrent access.
func (c *lruCache) put(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Cache disabled
	if c.maxSize == 0 {
		return
	}

	// Update existing entry
	if elem, ok := c.cache[key]; ok {
		c.lruList.MoveToFront(elem)
		elem.Value.(*cacheEntry).value = value
		return
	}

	// Evict if cache full
	if len(c.cache) >= c.maxSize {
		c.evictLRU()
	}

	// Add new entry
	entry := &cacheEntry{key: key, value: value}
	elem := c.lruList.PushFront(entry)
	c.cache[key] = elem
}

// evictLRU removes the least recently used entry from the cache.
// Must be called with write lock held (mu.Lock).
func (c *lruCache) evictLRU() {
	oldest := c.lruList.Back()
	if oldest != nil {
		c.lruList.Remove(oldest)
		entry := oldest.Value.(*cacheEntry)
		delete(c.cache, entry.key)
		c.evictions.Add(1)
	}
}

// Package-level cache singleton with default size of 1000 entries (~250KB memory).
var schemaCache = newLRUCache(1000)

// GetCacheStats returns a snapshot of current cache performance metrics.
// Thread-safe, non-blocking operation suitable for high-frequency monitoring.
//
// Example usage:
//
//	stats := analytics.GetCacheStats()
//	fmt.Printf("Cache: %d entries, %.1f%% hit rate, %d evictions\n",
//	    stats.Size, stats.HitRate*100, stats.Evictions)
//
// For Prometheus metrics export:
//
//	stats := analytics.GetCacheStats()
//	cacheHitsTotal.Set(float64(stats.Hits))
//	cacheSizeGauge.Set(float64(stats.Size))
func GetCacheStats() CacheStats {
	schemaCache.mu.RLock()
	size := len(schemaCache.cache)
	schemaCache.mu.RUnlock()

	hits := schemaCache.hits.Load()
	misses := schemaCache.misses.Load()
	evictions := schemaCache.evictions.Load()

	var hitRate float64
	total := hits + misses
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	return CacheStats{
		Size:      size,
		Hits:      hits,
		Misses:    misses,
		Evictions: evictions,
		HitRate:   hitRate,
	}
}

// SetSchemaCacheConfig configures the maximum cache size.
// maxSize=0 disables caching (all lookups miss).
// maxSize>0 sets the cache limit with LRU eviction.
// If new maxSize < current size, evicts LRU entries immediately.
// Thread-safe, can be called at runtime without restart.
//
// Example usage:
//
//	// High-cardinality workload
//	if err := analytics.SetSchemaCacheConfig(5000); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Disable cache for testing
//	analytics.SetSchemaCacheConfig(0)
//
// Returns error if maxSize < 0.
func SetSchemaCacheConfig(maxSize int) error {
	if maxSize < 0 {
		return fmt.Errorf("maxSize must be non-negative, got: %d", maxSize)
	}

	schemaCache.mu.Lock()
	defer schemaCache.mu.Unlock()

	schemaCache.maxSize = maxSize

	// Clear cache if disabled
	if maxSize == 0 {
		schemaCache.cache = make(map[string]*list.Element)
		schemaCache.lruList = list.New()
		return nil
	}

	// Evict entries if shrinking
	for len(schemaCache.cache) > maxSize {
		schemaCache.evictLRU()
	}

	return nil
}

// ClearSchemaCache clears all cached entries and resets performance metrics.
// Preserves maxSize configuration (does not disable caching).
// Thread-safe, useful for maintenance, testing, or after schema deployments.
//
// Example usage:
//
//	// After schema deployment
//	analytics.ClearSchemaCache()
//	log.Info("Schema cache cleared after deployment")
//
//	// Periodic reset
//	ticker := time.NewTicker(24 * time.Hour)
//	for range ticker.C {
//	    analytics.ClearSchemaCache()
//	}
func ClearSchemaCache() {
	schemaCache.mu.Lock()
	defer schemaCache.mu.Unlock()

	schemaCache.cache = make(map[string]*list.Element)
	schemaCache.lruList = list.New()
	schemaCache.hits.Store(0)
	schemaCache.misses.Store(0)
	schemaCache.evictions.Store(0)
}
