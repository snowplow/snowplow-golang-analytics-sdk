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
	Evictions uint64  // Total full-cache clears since initialization/clear
	HitRate   float64 // Calculated hit rate: hits/(hits+misses), range [0.0-1.0]
}

// schemaStore implements a thread-safe bounded cache.
// Reads use a shared read lock for concurrency; writes use an exclusive lock.
// When the cache is full on a put, the entire map is cleared before inserting
// the new entry (nuke strategy). With a sufficiently large maxSize this is
// rarely triggered in practice.
type schemaStore struct {
	maxSize   int               // Maximum number of entries (0 = disabled)
	cache     map[string]string // Key→value store
	mu        sync.RWMutex      // Protects cache map
	hits      atomic.Uint64     // Cache hit counter (lock-free)
	misses    atomic.Uint64     // Cache miss counter (lock-free)
	evictions atomic.Uint64     // Full-clear counter (lock-free)
}

func newSchemaStore(maxSize int) *schemaStore {
	return &schemaStore{
		maxSize: maxSize,
		cache:   make(map[string]string),
	}
}

// get retrieves a value from the cache by key.
// Uses a shared read lock so concurrent reads do not block each other.
func (c *schemaStore) get(key string) (string, bool) {
	c.mu.RLock()
	v, ok := c.cache[key]
	c.mu.RUnlock()

	if ok {
		c.hits.Add(1)
	} else {
		c.misses.Add(1)
	}
	return v, ok
}

// put adds a key-value pair to the cache.
// If the cache is full, the entire map is cleared first (nuke strategy).
func (c *schemaStore) put(key, value string) {
	if c.maxSize == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.cache) >= c.maxSize {
		c.cache = make(map[string]string)
		c.evictions.Add(1)
	}

	c.cache[key] = value
}

// Package-level cache singleton. Default size of 10,000 entries is large enough
// that the nuke eviction strategy is rarely triggered in practice.
var schemaCache = newSchemaStore(10000)

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
// maxSize>0 sets the cache limit; when full the entire cache is cleared on the next put.
// If new maxSize < current size, clears the cache immediately.
// Thread-safe, can be called at runtime without restart.
//
// Example usage:
//
//	// High-cardinality workload
//	if err := analytics.SetSchemaCacheConfig(50000); err != nil {
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

	if maxSize == 0 || len(schemaCache.cache) > maxSize {
		schemaCache.cache = make(map[string]string)
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

	schemaCache.cache = make(map[string]string)
	schemaCache.hits.Store(0)
	schemaCache.misses.Store(0)
	schemaCache.evictions.Store(0)
}
