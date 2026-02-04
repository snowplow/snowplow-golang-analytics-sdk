# Data Model: Bounded Schema Cache

**Feature**: 002-bounded-schema-cache  
**Date**: 2026-02-04  
**Purpose**: Define data structures for LRU cache implementation

## Overview

Three primary structures: `lruCache` (internal cache implementation), `cacheEntry` (cached data), and `CacheStats` (exported metrics).

---

## Core Structures

### lruCache (Internal)

**Purpose**: Thread-safe LRU cache with bounded size

**Structure**:
```go
type lruCache struct {
    maxSize   int                      // Maximum number of entries (0 = disabled)
    cache     map[string]*list.Element // O(1) lookup by key
    lruList   *list.List               // Doubly-linked list for LRU ordering
    mu        sync.RWMutex             // Thread-safety for concurrent access
    hits      atomic.Int64             // Cache hit counter
    misses    atomic.Int64             // Cache miss counter
    evictions atomic.Int64             // Eviction counter
}
```

**Attributes**:
- `maxSize`: Enforces bounded memory (default: 1000)
  - 0 = caching disabled (always miss)
  - >0 = maximum entries before eviction
- `cache`: Map from cache key to list element pointer
  - Key format: `"{prefix}:{schemaUri}"` (e.g., `"contexts:iglu:com.acme/widget/jsonschema/1-0-0"`)
  - Value: Pointer to list.Element for O(1) LRU updates
- `lruList`: Doubly-linked list maintaining LRU order
  - Front = most recently used
  - Back = least recently used (eviction candidate)
- `mu`: RWMutex for thread-safe concurrent access
  - Read lock: Not used (MoveToFront requires write)
  - Write lock: All operations (get, put, evict)
- `hits`: Atomic counter incremented on cache hit
- `misses`: Atomic counter incremented on cache miss
- `evictions`: Atomic counter incremented on eviction

**Relationships**:
- Contains 0 to maxSize cacheEntry instances
- Each cacheEntry exists in both cache map and lruList
- Package-level singleton: `var schemaCache = newLRUCache(1000)`

**Memory Footprint**:
- Base structure: ~100 bytes
- Per entry overhead: ~150 bytes (map entry + list.Element + pointers)
- Per entry data: ~100 bytes (key + value strings, average 50 chars each)
- **Total**: ~250 bytes per entry (maxSize * 250 = default 250KB)

---

### cacheEntry (Internal)

**Purpose**: Data stored in each list element

**Structure**:
```go
type cacheEntry struct {
    key   string  // Cache key ("{prefix}:{schemaUri}")
    value string  // Transformed schema name
}
```

**Attributes**:
- `key`: Original cache key for reverse lookup during eviction
  - Example: `"contexts:iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-1"`
- `value`: Computed schema transformation result
  - Example: `"contexts_com_snowplowanalytics_snowplow_client_session_1"`

**Why key in entry?**
- During eviction, we have list.Element but need map key for deletion
- Storing key avoids reverse map lookup

**Storage Location**:
```go
element := list.Element{
    Value: &cacheEntry{key: k, value: v}, // Pointer to avoid copying
}
```

**Relationships**:
- Owned by lruCache
- Referenced by both cache map and lruList
- Lifecycle: Created on cache miss, evicted when LRU + cache full

---

### CacheStats (Exported)

**Purpose**: Performance metrics snapshot for monitoring

**Structure**:
```go
// CacheStats represents cache performance metrics.
// Obtain via GetCacheStats() for monitoring and tuning.
type CacheStats struct {
    Hits      int64   // Number of cache hits
    Misses    int64   // Number of cache misses
    Evictions int64   // Number of entries evicted due to size limit
    Size      int64   // Current number of cached entries
    HitRate   float64 // Calculated hit rate: Hits / (Hits + Misses)
}
```

**Attributes**:
- `Hits`: Total cache hits since initialization
  - Incremented on successful get() operation
- `Misses`: Total cache misses since initialization
  - Incremented on failed get() operation
- `Evictions`: Total evictions due to maxSize limit
  - Incremented each time oldest entry removed
- `Size`: Current cache size (0 to maxSize)
  - Snapshot of lruList.Len() at call time
- `HitRate`: Calculated hit rate percentage (0.0 to 1.0)
  - Formula: `Hits / (Hits + Misses)` (0 if no accesses yet)

**Usage**:
```go
stats := analytics.GetCacheStats()
if stats.HitRate < 0.90 && stats.Size >= 1000 {
    log.Warn("Low hit rate - consider increasing maxSize")
}
```

**Relationships**:
- Read-only snapshot from lruCache internal state
- No back-reference to cache (immutable value type)
- Returned by GetCacheStats() API

---

## Data Flows

### Cache Hit Flow

```
fixSchema() -> get(key)
                 ↓
         [Check cache map]
                 ↓
         [Key exists] → MoveToFront() → Return value
                 ↓
         [Increment hits]
```

**Operations**:
1. Acquire write lock (mu.Lock)
2. Lookup key in cache map
3. If found:
   - Get list.Element from map value
   - Call lruList.MoveToFront(element) - O(1)
   - Extract value from element.Value.(*cacheEntry).value
   - Increment hits counter atomically
4. Release lock (mu.Unlock)
5. Return value

**Complexity**: O(1)

### Cache Miss Flow

```
fixSchema() -> get(key) → [Miss] → Compute result → put(key, value)
                 ↓                                          ↓
         [Key not found]                           [Add to cache]
                 ↓                                          ↓
         [Increment misses]                         [Evict if full]
```

**Operations**:
1. **get(key)**:
   - Acquire write lock
   - Lookup fails
   - Increment misses counter
   - Release lock
   - Return (value, false)

2. **Compute result** (outside cache):
   - extractSchema()
   - String transformations
   - Build result string

3. **put(key, value)**:
   - Acquire write lock
   - Check if key already exists (race condition guard)
   - If cache full (lruList.Len() >= maxSize && maxSize > 0):
     - Call evictOldest()
   - Create new cacheEntry
   - Add to front of lruList (PushFront)
   - Store element pointer in cache map
   - Release lock

**Complexity**: O(1)

### Eviction Flow

```
put(key, value) → [Cache full] → evictOldest()
                                       ↓
                               [Get Back() element]
                                       ↓
                               [Extract key from entry]
                                       ↓
                               [delete from map]
                                       ↓
                               [Remove from list]
                                       ↓
                               [Increment evictions]
```

**Operations**:
1. Get oldest element: `oldest := lruList.Back()`
2. Extract key: `entry := oldest.Value.(*cacheEntry)`
3. Remove from map: `delete(cache, entry.key)`
4. Remove from list: `lruList.Remove(oldest)` - O(1)
5. Increment evictions counter
6. GC handles freed memory automatically

**Complexity**: O(1)

---

## Invariants

### Consistency Invariants (must hold at all times)

1. **Size Invariant**: `lruList.Len() <= maxSize` (unless maxSize = 0)
2. **Bidirectional Mapping**: Every cache map entry has corresponding list element
3. **Reverse Mapping**: Every list element's key exists in cache map
4. **LRU Ordering**: List front = most recently accessed, back = least recently accessed
5. **Key Uniqueness**: Each key appears at most once in cache

### Thread-Safety Invariants

1. **Mutual Exclusion**: All cache operations protected by mu lock
2. **Atomic Counters**: hits, misses, evictions updated atomically
3. **No Deadlocks**: Single lock, no nested locking, always deferred unlock

### Performance Invariants

1. **O(1) Operations**: get(), put(), evict() all constant time
2. **Zero Allocations on Hit**: MoveToFront reuses existing element
3. **Bounded Memory**: Total memory ≤ (maxSize * 250 bytes)

---

## Configuration States

### State 1: Default (maxSize = 1000)
- **Behavior**: LRU eviction after 1000 entries
- **Memory**: ~250KB maximum
- **Use Case**: Standard production workloads (<1000 unique schemas)

### State 2: Disabled (maxSize = 0)
- **Behavior**: No caching, all lookups miss, put() is no-op
- **Memory**: Minimal (empty structures)
- **Use Case**: Testing, debugging, extreme high-cardinality

### State 3: High Capacity (maxSize = 5000)
- **Behavior**: LRU eviction after 5000 entries
- **Memory**: ~1.25MB maximum
- **Use Case**: Multi-tenant platforms, high schema diversity

### State 4: Low Capacity (maxSize = 100)
- **Behavior**: Frequent evictions, lower hit rate
- **Memory**: ~25KB maximum
- **Use Case**: Memory-constrained environments

---

## Example Scenarios

### Scenario 1: Cache Fill (0 → 1000 entries)

**Initial State**:
- Size: 0
- Hits: 0, Misses: 0, Evictions: 0

**Process 1000 unique schemas**:
- First access: miss → put() → size=1, front=schema1
- Second access: miss → put() → size=2, front=schema2
- ...
- 1000th access: miss → put() → size=1000, front=schema1000

**Final State**:
- Size: 1000 (at limit)
- Hits: 0, Misses: 1000, Evictions: 0
- List: [schema1000, schema999, ..., schema1] (front to back)

### Scenario 2: Eviction (1001st unique schema)

**Initial State** (from Scenario 1):
- Size: 1000, cache full

**Process 1001st schema**:
1. get("schema1001") → miss (misses=1001)
2. put("schema1001", value):
   - Cache full (1000 >= 1000)
   - evictOldest():
     - Back element = schema1
     - delete(cache, "schema1")
     - Remove from list
     - evictions=1
   - PushFront(schema1001)
   - cache["schema1001"] = element

**Final State**:
- Size: 1000 (still at limit)
- Hits: 0, Misses: 1001, Evictions: 1
- List: [schema1001, schema1000, ..., schema2] (schema1 evicted)

### Scenario 3: LRU Update (access existing entry)

**Initial State**:
- Size: 1000
- List: [schemaA, schemaB, schemaC, ..., schemaZ]

**Access schemaZ** (currently at back):
1. get("schemaZ") → hit (hits=1)
2. MoveToFront(schemaZ element)

**Final State**:
- Size: 1000 (unchanged)
- Hits: 1
- List: [schemaZ, schemaA, schemaB, schemaC, ..., schemaY] (schemaZ moved to front)

### Scenario 4: Disable Caching

**Action**: `SetSchemaCacheConfig(0)`

**Behavior**:
- get() always returns false (miss)
- put() is no-op (never stores)
- Size remains 0
- Misses increment on every access

---

## Data Model Validation

✅ **Completeness**: All structures defined with clear purpose  
✅ **Relationships**: Bidirectional mapping between map and list documented  
✅ **Invariants**: Consistency and thread-safety guarantees specified  
✅ **Scenarios**: Common flows illustrated with state transitions  
✅ **Memory**: Footprint calculated and documented

**Status**: Ready for implementation
