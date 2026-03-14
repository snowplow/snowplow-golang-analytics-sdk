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
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// US1 Tests: Bounded Memory Consumption

func TestLRUCacheBasicOperations(t *testing.T) {
	cache := newLRUCache(10)

	// Happy path: add and retrieve
	cache.put("key1", "value1")
	val, ok := cache.get("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)

	// Edge case: non-existent key
	val, ok = cache.get("nonexistent")
	assert.False(t, ok)
	assert.Equal(t, "", val)

	// Edge case: empty cache operations
	emptyCache := newLRUCache(10)
	val, ok = emptyCache.get("any")
	assert.False(t, ok)
	assert.Equal(t, "", val)
}

func TestLRUCacheEviction(t *testing.T) {
	cache := newLRUCache(3)

	// Fill cache to maxSize
	cache.put("key1", "value1")
	cache.put("key2", "value2")
	cache.put("key3", "value3")
	assert.Equal(t, 3, len(cache.cache))

	// Add one more - should evict oldest (key1)
	cache.put("key4", "value4")
	assert.Equal(t, 3, len(cache.cache))

	// Verify key1 was evicted
	_, ok := cache.get("key1")
	assert.False(t, ok)

	// Verify others still exist
	_, ok = cache.get("key2")
	assert.True(t, ok)
	_, ok = cache.get("key3")
	assert.True(t, ok)
	_, ok = cache.get("key4")
	assert.True(t, ok)

	// Verify eviction counter
	assert.Equal(t, uint64(1), cache.evictions.Load())

	// Verify size never exceeds maxSize
	for i := 0; i < 100; i++ {
		cache.put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		require.LessOrEqual(t, len(cache.cache), 3)
	}
}

func TestLRUCacheLRUOrdering(t *testing.T) {
	cache := newLRUCache(3)

	// Add 3 entries
	cache.put("key1", "value1")
	cache.put("key2", "value2")
	cache.put("key3", "value3")

	// Access middle entry (key2) - moves to front
	cache.get("key2")

	// Add new entry - should evict key1 (oldest), not key2
	cache.put("key4", "value4")

	// Verify key1 evicted, key2 still present
	_, ok := cache.get("key1")
	assert.False(t, ok)
	_, ok = cache.get("key2")
	assert.True(t, ok)

	// Repeated access keeps entry at front
	cache.get("key3")
	cache.get("key3")
	cache.get("key3")
	cache.put("key5", "value5")

	// key4 should be evicted (oldest), key3 still present
	_, ok = cache.get("key4")
	assert.False(t, ok)
	_, ok = cache.get("key3")
	assert.True(t, ok)
}

func TestLRUCacheThreadSafety(t *testing.T) {
	cache := newLRUCache(100)
	var wg sync.WaitGroup

	// 100 goroutines simultaneously reading/writing
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", idx%20) // Create contention
			value := fmt.Sprintf("value%d", idx)

			cache.put(key, value)
			cache.get(key)
		}(i)
	}

	wg.Wait()

	// Verify cache state is consistent
	assert.LessOrEqual(t, len(cache.cache), 100)
	assert.Greater(t, cache.hits.Load()+cache.misses.Load(), uint64(0))
}

func TestLRUCacheMemoryBounds(t *testing.T) {
	// Measure memory with 1000 entries
	cache := newLRUCache(1000)

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	baseline := m1.Alloc

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("iglu:com.test/schema_%d/jsonschema/1-0-0", i)
		value := fmt.Sprintf("contexts_com_test_schema_%d_1", i)
		cache.put(key, value)
	}

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	after := m2.Alloc

	var delta uint64
	if after > baseline {
		delta = after - baseline
	} else {
		// GC may have reclaimed more than we allocated
		t.Skip("GC reclaimed memory, skipping memory measurement")
		return
	}

	perEntry := delta / 1000

	// Verify reasonable per-entry memory (~250 bytes target, allow up to 500 bytes with overhead)
	assert.LessOrEqual(t, perEntry, uint64(500), "Per-entry memory exceeds 500 bytes: %d bytes", perEntry)

	t.Logf("Memory per entry: %d bytes (target: ~250 bytes)", perEntry)
	t.Logf("Total cache memory: %d bytes for 1000 entries", delta)
}

// US2 Tests: Cache Observability

func TestGetCacheStatsEmpty(t *testing.T) {
	cache := newLRUCache(10)
	stats := getCacheStats(cache)

	assert.Equal(t, 0, stats.Size)
	assert.Equal(t, uint64(0), stats.Hits)
	assert.Equal(t, uint64(0), stats.Misses)
	assert.Equal(t, uint64(0), stats.Evictions)
	assert.Equal(t, 0.0, stats.HitRate)
}

func TestGetCacheStatsHits(t *testing.T) {
	cache := newLRUCache(10)
	cache.put("key1", "value1")

	// Access 5 times
	for i := 0; i < 5; i++ {
		cache.get("key1")
	}

	stats := getCacheStats(cache)
	assert.Equal(t, uint64(5), stats.Hits)
	assert.Equal(t, uint64(0), stats.Misses)
	assert.Equal(t, 1.0, stats.HitRate)
}

func TestGetCacheStatsMisses(t *testing.T) {
	cache := newLRUCache(10)

	// 10 misses
	for i := 0; i < 10; i++ {
		cache.get(fmt.Sprintf("miss%d", i))
	}

	stats := getCacheStats(cache)
	assert.Equal(t, uint64(0), stats.Hits)
	assert.Equal(t, uint64(10), stats.Misses)
	assert.Equal(t, 0.0, stats.HitRate)
}

func TestGetCacheStatsEvictions(t *testing.T) {
	cache := newLRUCache(10)

	// Fill cache
	for i := 0; i < 10; i++ {
		cache.put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	// Add 5 more - should evict 5
	for i := 10; i < 15; i++ {
		cache.put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	stats := getCacheStats(cache)
	assert.Equal(t, uint64(5), stats.Evictions)
}

func TestGetCacheStatsHitRate(t *testing.T) {
	cache := newLRUCache(10)
	cache.put("key1", "value1")

	// 80 hits
	for i := 0; i < 80; i++ {
		cache.get("key1")
	}

	// 20 misses
	for i := 0; i < 20; i++ {
		cache.get(fmt.Sprintf("miss%d", i))
	}

	stats := getCacheStats(cache)
	assert.Equal(t, uint64(80), stats.Hits)
	assert.Equal(t, uint64(20), stats.Misses)
	assert.InDelta(t, 0.8, stats.HitRate, 0.01)
}

func TestGetCacheStatsThreadSafe(t *testing.T) {
	cache := newLRUCache(100)
	cache.put("key1", "value1")

	var wg sync.WaitGroup
	// 100 goroutines calling GetCacheStats concurrently
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats := getCacheStats(cache)
			assert.GreaterOrEqual(t, stats.Size, 0)
		}()
	}

	wg.Wait()
}

// Helper for testing GetCacheStats on arbitrary cache
func getCacheStats(cache *lruCache) CacheStats {
	cache.mu.RLock()
	size := len(cache.cache)
	cache.mu.RUnlock()

	hits := cache.hits.Load()
	misses := cache.misses.Load()
	evictions := cache.evictions.Load()

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

// US3 Tests: Configurable Cache Behavior

func TestSetSchemaCacheConfigValid(t *testing.T) {
	// Valid configurations
	assert.Nil(t, SetSchemaCacheConfig(500))
	assert.Nil(t, SetSchemaCacheConfig(0)) // Disable
	assert.Nil(t, SetSchemaCacheConfig(5000))

	// Restore default
	SetSchemaCacheConfig(1000)
}

func TestSetSchemaCacheConfigInvalid(t *testing.T) {
	err := SetSchemaCacheConfig(-1)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "maxSize must be non-negative, got: -1")

	err = SetSchemaCacheConfig(-100)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "maxSize must be non-negative")
}

func TestSetSchemaCacheConfigShrink(t *testing.T) {
	// Setup: Fill cache with 1000 entries
	SetSchemaCacheConfig(1000)
	ClearSchemaCache()

	for i := 0; i < 1000; i++ {
		schemaCache.put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	stats1 := GetCacheStats()
	assert.Equal(t, 1000, stats1.Size)

	// Shrink to 500
	SetSchemaCacheConfig(500)

	stats2 := GetCacheStats()
	assert.Equal(t, 500, stats2.Size)
	assert.Equal(t, uint64(500), stats2.Evictions)

	// Restore default
	SetSchemaCacheConfig(1000)
}

func TestSetSchemaCacheConfigGrow(t *testing.T) {
	// Setup: Small cache
	SetSchemaCacheConfig(100)
	ClearSchemaCache()

	for i := 0; i < 100; i++ {
		schemaCache.put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	// Grow to 1000
	SetSchemaCacheConfig(1000)

	// Should be able to hold 1000 entries now
	for i := 100; i < 1000; i++ {
		schemaCache.put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	stats := GetCacheStats()
	assert.Equal(t, 1000, stats.Size)

	// Restore default
	SetSchemaCacheConfig(1000)
}

func TestSetSchemaCacheConfigDisable(t *testing.T) {
	SetSchemaCacheConfig(0) // Disable
	ClearSchemaCache()

	schemaCache.put("key1", "value1")

	// All gets should miss
	_, ok := schemaCache.get("key1")
	assert.False(t, ok)

	stats := GetCacheStats()
	assert.Equal(t, 0, stats.Size)

	// Restore default
	SetSchemaCacheConfig(1000)
}

func TestSetSchemaCacheConfigConcurrent(t *testing.T) {
	var wg sync.WaitGroup

	// Multiple goroutines calling SetSchemaCacheConfig
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			size := 100 + idx*100
			SetSchemaCacheConfig(size)
		}(i)
	}

	wg.Wait()

	// Last call should win (one of 100-1000)
	assert.GreaterOrEqual(t, schemaCache.maxSize, 100)

	// Restore default
	SetSchemaCacheConfig(1000)
}

func TestClearSchemaCacheBasic(t *testing.T) {
	ClearSchemaCache()

	// Add 100 entries
	for i := 0; i < 100; i++ {
		schemaCache.put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	// Generate some hits/misses
	schemaCache.get("key1")
	schemaCache.get("nonexistent")

	stats1 := GetCacheStats()
	assert.Equal(t, 100, stats1.Size)
	assert.Greater(t, stats1.Hits+stats1.Misses, uint64(0))

	// Clear cache
	ClearSchemaCache()

	stats2 := GetCacheStats()
	assert.Equal(t, 0, stats2.Size)
	assert.Equal(t, uint64(0), stats2.Hits)
	assert.Equal(t, uint64(0), stats2.Misses)
	assert.Equal(t, uint64(0), stats2.Evictions)

	// Verify maxSize preserved
	assert.Equal(t, 1000, schemaCache.maxSize)
}

func TestClearSchemaCacheThreadSafe(t *testing.T) {
	var wg sync.WaitGroup

	// Populate cache
	for i := 0; i < 50; i++ {
		schemaCache.put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	// Concurrent clears and operations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ClearSchemaCache()
		}()

		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			schemaCache.put(fmt.Sprintf("key%d", idx), fmt.Sprintf("value%d", idx))
			schemaCache.get(fmt.Sprintf("key%d", idx))
		}(i)
	}

	wg.Wait()

	// Should complete without panics
	stats := GetCacheStats()
	assert.GreaterOrEqual(t, stats.Size, 0)
}
