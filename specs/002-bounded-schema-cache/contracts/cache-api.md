# API Contracts: Schema Cache Management

**Feature**: 002-bounded-schema-cache  
**Version**: v0.4.2  
**Package**: `github.com/snowplow/snowplow-golang-analytics-sdk/analytics`

---

## Overview

Three public APIs for schema cache observability and configuration:

1. **GetCacheStats()** - Performance metrics
2. **SetSchemaCacheConfig(maxSize)** - Runtime configuration
3. **ClearSchemaCache()** - Manual cache invalidation

All APIs are **thread-safe** and **production-ready**.

---

## API: GetCacheStats()

### Signature

```go
func GetCacheStats() CacheStats
```

### Description

Returns a snapshot of current cache performance metrics. Non-blocking, read-only operation suitable for high-frequency monitoring.

### Return Type

```go
type CacheStats struct {
    Size      int     // Current number of cached entries
    Hits      uint64  // Total cache hits since initialization/clear
    Misses    uint64  // Total cache misses since initialization/clear
    Evictions uint64  // Total LRU evictions since initialization/clear
    HitRate   float64 // Calculated hit rate: hits/(hits+misses) [0.0-1.0]
}
```

### Behavior

- **Thread-Safe**: Uses atomic reads for counters (hits, misses, evictions)
- **Performance**: <10ns overhead (atomic loads only)
- **Snapshot Consistency**: Counters may increment between fields (eventual consistency acceptable for monitoring)
- **HitRate Calculation**:
  - If `hits + misses == 0`: Returns `0.0`
  - Otherwise: Returns `float64(hits) / float64(hits + misses)`
  - Range: `[0.0, 1.0]` where `1.0 = 100%` hit rate

### Examples

#### Basic Monitoring

```go
stats := analytics.GetCacheStats()
fmt.Printf("Cache: %d entries, %.1f%% hit rate\n", 
    stats.Size, stats.HitRate*100)
// Output: Cache: 847 entries, 98.3% hit rate
```

#### Alerting Logic

```go
stats := analytics.GetCacheStats()
if stats.HitRate < 0.90 {
    alerting.Warn("Schema cache hit rate below 90%: %.1f%%", 
        stats.HitRate*100)
}
```

#### Prometheus Export

```go
stats := analytics.GetCacheStats()
cacheHitsTotal.Set(float64(stats.Hits))
cacheMissesTotal.Set(float64(stats.Misses))
cacheEvictionsTotal.Set(float64(stats.Evictions))
cacheSizeGauge.Set(float64(stats.Size))
cacheHitRateGauge.Set(stats.HitRate)
```

### Edge Cases

| Scenario | Behavior |
|----------|----------|
| **Cache disabled** (maxSize=0) | Size=0, HitRate=0.0, Misses increment |
| **No operations yet** | Size=0, Hits=0, Misses=0, HitRate=0.0 |
| **After ClearSchemaCache()** | Size=0, counters reset to 0 |
| **Concurrent reads** | May see intermediate values (acceptable) |

### Testing Hooks

```go
// Validate counter increments
stats1 := analytics.GetCacheStats()
_, _ = analytics.ParseEvent(event) // Triggers cache lookup
stats2 := analytics.GetCacheStats()

assert.Equal(t, stats1.Hits+1, stats2.Hits) // Cache hit
// OR
assert.Equal(t, stats1.Misses+1, stats2.Misses) // Cache miss
```

---

## API: SetSchemaCacheConfig(maxSize int)

### Signature

```go
func SetSchemaCacheConfig(maxSize int) error
```

### Description

Configures maximum cache size. Can be called at initialization or runtime to adjust cache limits dynamically.

### Parameters

| Parameter | Type | Description | Constraints |
|-----------|------|-------------|-------------|
| `maxSize` | `int` | Maximum cache entries | `maxSize >= 0` |

### Return Value

| Type | Value | Condition |
|------|-------|-----------|
| `error` | `nil` | Success (maxSize >= 0) |
| `error` | `fmt.Errorf("maxSize must be non-negative, got: %d", maxSize)` | maxSize < 0 |

### Behavior

- **Thread-Safe**: Acquires write lock during reconfiguration
- **Runtime Reconfigurable**: Can be called multiple times
- **Eviction on Shrink**: If new maxSize < current size, evicts LRU entries immediately
- **Growth**: If new maxSize > current size, allows cache to grow naturally
- **Disable**: Setting maxSize=0 disables caching (all lookups miss)
- **Performance**: O(n) where n = entries to evict (if shrinking)

### Configuration Values

| maxSize | Behavior | Memory | Use Case |
|---------|----------|--------|----------|
| **0** | Caching disabled | 0 bytes | Testing, debugging |
| **100** | Minimal cache | ~25KB | Embedded systems |
| **1000** (default) | Standard cache | ~250KB | Typical workloads |
| **5000** | Large cache | ~1.25MB | High schema diversity |
| **10000+** | Very large cache | ~2.5MB+ | Multi-tenant platforms |

### Examples

#### Initialization

```go
func main() {
    // Configure before processing events
    if err := analytics.SetSchemaCacheConfig(5000); err != nil {
        log.Fatalf("Failed to configure cache: %v", err)
    }
    
    // Process events...
}
```

#### Runtime Adjustment

```go
// Monitor and adjust
go func() {
    ticker := time.NewTicker(5 * time.Minute)
    for range ticker.C {
        stats := analytics.GetCacheStats()
        
        if stats.HitRate < 0.90 && stats.Size >= currentMaxSize {
            newMaxSize := currentMaxSize * 2
            if err := analytics.SetSchemaCacheConfig(newMaxSize); err != nil {
                log.Errorf("Failed to increase cache: %v", err)
            } else {
                log.Infof("Cache increased: %d -> %d", currentMaxSize, newMaxSize)
                currentMaxSize = newMaxSize
            }
        }
    }
}()
```

#### Disable for Testing

```go
func TestSchemaTransform(t *testing.T) {
    // Disable cache for deterministic tests
    if err := analytics.SetSchemaCacheConfig(0); err != nil {
        t.Fatal(err)
    }
    defer analytics.SetSchemaCacheConfig(1000) // Restore
    
    // Test schema transformations...
}
```

### Error Handling

```go
// Validate input
if err := analytics.SetSchemaCacheConfig(-100); err != nil {
    // Error: "maxSize must be non-negative, got: -100"
    log.Error(err)
}

// Success
if err := analytics.SetSchemaCacheConfig(0); err == nil {
    // Caching disabled
}
```

### Edge Cases

| Scenario | Behavior |
|----------|----------|
| **maxSize < 0** | Returns error, no state change |
| **maxSize == current** | No-op, returns nil |
| **maxSize > current** | Allows growth, returns nil |
| **maxSize < current size** | Evicts LRU entries until size <= maxSize |
| **maxSize == 0** | Clears all entries, disables caching |
| **Concurrent calls** | Serialized via write lock (last call wins) |

### Testing Hooks

```go
// Test shrinking
analytics.SetSchemaCacheConfig(1000)
// ... populate cache with 1000 entries
stats1 := analytics.GetCacheStats()
assert.Equal(t, 1000, stats1.Size)

analytics.SetSchemaCacheConfig(500)
stats2 := analytics.GetCacheStats()
assert.Equal(t, 500, stats2.Size)
assert.Equal(t, uint64(500), stats2.Evictions)
```

---

## API: ClearSchemaCache()

### Signature

```go
func ClearSchemaCache()
```

### Description

Clears all cached schema entries and resets performance metrics (hits, misses, evictions). Does NOT change maxSize configuration.

### Parameters

None

### Return Value

None (always succeeds)

### Behavior

- **Thread-Safe**: Acquires write lock during clear
- **Clears Entries**: Removes all cached schema mappings
- **Resets Metrics**: Sets hits=0, misses=0, evictions=0
- **Preserves Config**: maxSize unchanged
- **Performance**: O(1) - replaces map and list, GC reclaims old entries

### Examples

#### After Schema Deployment

```go
func deploySchemas() {
    // Deploy new schemas to registry
    if err := updateSchemaRegistry(); err != nil {
        return err
    }
    
    // Clear cache to force recomputation with new schemas
    analytics.ClearSchemaCache()
    log.Info("Schema cache cleared after deployment")
}
```

#### Periodic Reset

```go
// Reset cache daily to prevent stale entries
func scheduleCacheReset() {
    ticker := time.NewTicker(24 * time.Hour)
    defer ticker.Stop()
    
    for range ticker.C {
        oldStats := analytics.GetCacheStats()
        analytics.ClearSchemaCache()
        
        log.Infof("Daily cache reset: %d entries cleared, %.1f%% hit rate",
            oldStats.Size, oldStats.HitRate*100)
    }
}
```

#### Manual Troubleshooting

```go
// Clear cache to rule out stale entries
func debugSchemaIssue() {
    log.Info("Clearing schema cache for debugging")
    analytics.ClearSchemaCache()
    
    // Retry problematic event
    event, err := analytics.ParseEvent(problematicEvent)
    if err != nil {
        log.Errorf("Error persists after cache clear: %v", err)
    }
}
```

### Edge Cases

| Scenario | Behavior |
|----------|----------|
| **Cache already empty** | No-op (metrics remain 0) |
| **Cache disabled** (maxSize=0) | Resets metrics (no entries to clear) |
| **Concurrent access** | Blocked during clear (write lock) |
| **Multiple calls** | Each call clears and resets independently |

### Testing Hooks

```go
// Validate clear operation
// ... populate cache
stats1 := analytics.GetCacheStats()
assert.Greater(t, stats1.Size, 0)

analytics.ClearSchemaCache()

stats2 := analytics.GetCacheStats()
assert.Equal(t, 0, stats2.Size)
assert.Equal(t, uint64(0), stats2.Hits)
assert.Equal(t, uint64(0), stats2.Misses)
assert.Equal(t, uint64(0), stats2.Evictions)
```

---

## Thread Safety

All three APIs are **fully thread-safe**:

| API | Concurrency Mechanism | Performance |
|-----|----------------------|-------------|
| **GetCacheStats()** | Atomic reads (hits, misses, evictions), RLock for size | <10ns overhead |
| **SetSchemaCacheConfig()** | Write lock during reconfiguration | O(evictions) block time |
| **ClearSchemaCache()** | Write lock during clear | O(1) block time |

### Locking Strategy

```go
// Conceptual lock hierarchy (not exposed)
type lruCache struct {
    mu sync.RWMutex  // Protects cache map and list
    
    hits      atomic.Uint64  // Lock-free reads
    misses    atomic.Uint64  // Lock-free reads
    evictions atomic.Uint64  // Lock-free reads
}

// GetCacheStats: RLock for size, atomic for counters
// SetSchemaCacheConfig: Lock (write)
// ClearSchemaCache: Lock (write)
```

### Concurrent Access Guarantees

- **No deadlocks**: Single mutex, no nested locks
- **No race conditions**: Atomic counters, mutex-protected structures
- **Eventual consistency**: GetCacheStats() may see counters increment between fields (acceptable for monitoring)
- **Bounded blocking**: Write operations (SetConfig, Clear) block reads briefly (<1ms typical)

---

## Performance Characteristics

| Operation | Latency | Allocations | Blocking |
|-----------|---------|-------------|----------|
| **GetCacheStats()** | <10ns | 0 | RLock (shared) |
| **SetSchemaCacheConfig(grow)** | <100ns | 0 | Lock (exclusive) |
| **SetSchemaCacheConfig(shrink)** | O(evictions) | 0 | Lock (exclusive) |
| **ClearSchemaCache()** | <50ns | 1 (new map) | Lock (exclusive) |

---

## Error Handling

| API | Error Conditions | Error Message |
|-----|------------------|---------------|
| **GetCacheStats()** | Never fails | N/A |
| **SetSchemaCacheConfig()** | maxSize < 0 | `"maxSize must be non-negative, got: {maxSize}"` |
| **ClearSchemaCache()** | Never fails | N/A |

---

## Versioning

| Version | Change |
|---------|--------|
| **v0.4.2** | Initial release (all three APIs) |

Future versions will maintain **backward compatibility**:
- No signature changes
- No behavior changes for valid inputs
- New fields in CacheStats struct will be additive only

---

## Testing Contracts

### Unit Test Coverage (Required)

- **GetCacheStats()**:
  - Returns zero values for empty cache
  - Increments hits on cache hit
  - Increments misses on cache miss
  - Increments evictions on LRU eviction
  - Calculates hit rate correctly
  - Thread-safe with concurrent readers

- **SetSchemaCacheConfig()**:
  - Rejects negative maxSize
  - Accepts zero (disables cache)
  - Accepts positive values (configures limit)
  - Evicts entries when shrinking
  - Allows growth when expanding
  - Thread-safe with concurrent config changes

- **ClearSchemaCache()**:
  - Clears all entries
  - Resets metrics to zero
  - Preserves maxSize configuration
  - Thread-safe with concurrent clears

### Integration Test Coverage (Recommended)

- Monitor cache during realistic workload
- Validate hit rate >90% for typical schemas
- Verify memory bounds (maxSize × 250 bytes)
- Test runtime reconfiguration (grow/shrink/disable)
- Validate metrics export to monitoring systems

---

## Implementation Notes

### Internal Structures (Not Exposed)

```go
// lruCache is internal, not part of public API
type lruCache struct {
    maxSize   int
    cache     map[string]*list.Element
    lruList   *list.List
    mu        sync.RWMutex
    hits      atomic.Uint64
    misses    atomic.Uint64
    evictions atomic.Uint64
}

// cacheEntry is internal
type cacheEntry struct {
    key   string
    value map[string]interface{}
}
```

### Public Structures (Exposed)

```go
// CacheStats is the only exported type
type CacheStats struct {
    Size      int     // Exported
    Hits      uint64  // Exported
    Misses    uint64  // Exported
    Evictions uint64  // Exported
    HitRate   float64 // Exported (computed)
}
```

---

## API Changelog

### v0.4.2 (2026-02-04)

**Added**:
- `GetCacheStats() CacheStats` - Observability API
- `SetSchemaCacheConfig(maxSize int) error` - Configuration API
- `ClearSchemaCache()` - Maintenance API
- `CacheStats` struct - Metrics type

**Changed**: None (new feature)

**Deprecated**: None

**Removed**: None

**Breaking Changes**: None

---

**Document Version**: v0.4.2  
**Last Updated**: 2026-02-04  
**Contract Status**: FINAL
