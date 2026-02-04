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
	"testing"
)

// US1 Benchmarks: Cache Performance

func BenchmarkLRUCacheHit(b *testing.B) {
	cache := newLRUCache(1000)
	cache.put("test_key", "test_value")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.get("test_key")
	}
}

func BenchmarkLRUCacheMiss(b *testing.B) {
	cache := newLRUCache(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.get(fmt.Sprintf("miss_%d", i))
	}
}

func BenchmarkLRUEviction(b *testing.B) {
	cache := newLRUCache(100)

	// Fill cache
	for i := 0; i < 100; i++ {
		cache.put(fmt.Sprintf("key_%d", i), "value")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.put(fmt.Sprintf("evict_%d", i), "value")
	}
}

func BenchmarkLRUCacheChurn(b *testing.B) {
	cache := newLRUCache(1000)

	// Generate 2000 unique schemas (50% miss rate initially)
	schemas := make([]string, 2000)
	for i := 0; i < 2000; i++ {
		schemas[i] = fmt.Sprintf("iglu:com.test/schema_%d/jsonschema/1-0-0", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := schemas[i%len(schemas)]
		if _, ok := cache.get(key); !ok {
			cache.put(key, fmt.Sprintf("transformed_%d", i%len(schemas)))
		}
	}
}

func BenchmarkLRUConcurrent(b *testing.B) {
	cache := newLRUCache(1000)
	cache.put("shared_key", "value")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.get("shared_key")
		}
	})
}

// US2 Benchmarks: Observability Performance

func BenchmarkGetCacheStats(b *testing.B) {
	cache := newLRUCache(1000)
	cache.put("key1", "value1")
	cache.get("key1")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		getCacheStats(cache)
	}
}

// US3 Benchmarks: Configuration Performance

func BenchmarkSetSchemaCacheConfig(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		SetSchemaCacheConfig(1000 + i%1000)
	}

	// Restore default
	SetSchemaCacheConfig(1000)
}

func BenchmarkClearSchemaCache(b *testing.B) {
	// Populate cache between clears
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for j := 0; j < 100; j++ {
			schemaCache.put(fmt.Sprintf("key%d", j), fmt.Sprintf("value%d", j))
		}
		b.StartTimer()

		ClearSchemaCache()
	}
}
