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
	"testing"

	"github.com/stretchr/testify/assert"
)

func resetGlobalCache(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		SetSchemaCacheSize(10000)
	})
}

func TestCacheGetPut(t *testing.T) {
	c := newSchemaMapCache(10)

	c.put("k1", "v1")
	v, ok := c.get("k1")
	assert.True(t, ok)
	assert.Equal(t, "v1", v)

	v, ok = c.get("missing")
	assert.False(t, ok)
	assert.Equal(t, "", v)
}

func TestCacheEviction(t *testing.T) {
	c := newSchemaMapCache(3)

	c.put("k1", "v1")
	c.put("k2", "v2")
	c.put("k3", "v3")

	// Next put triggers purge
	c.put("k4", "v4")

	_, ok := c.get("k1")
	assert.False(t, ok)

	v, ok := c.get("k4")
	assert.True(t, ok)
	assert.Equal(t, "v4", v)
}

func TestCacheDisabled(t *testing.T) {
	c := newSchemaMapCache(0)

	c.put("k1", "v1")
	_, ok := c.get("k1")
	assert.False(t, ok)
}

func TestCacheConcurrent(t *testing.T) {
	c := newSchemaMapCache(1000)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", idx)
			c.put(key, fmt.Sprintf("val%d", idx))
			v, ok := c.get(key)
			assert.True(t, ok, "key %s should be present", key)
			assert.Equal(t, fmt.Sprintf("val%d", idx), v)
		}(i)
	}

	wg.Wait()
}

func TestSetSchemaCacheSize(t *testing.T) {
	resetGlobalCache(t)

	SetSchemaCacheSize(0)
	schemaCache.put("k1", "v1")
	_, ok := schemaCache.get("k1")
	assert.False(t, ok)

	SetSchemaCacheSize(100)
	schemaCache.put("k2", "v2")
	v, ok := schemaCache.get("k2")
	assert.True(t, ok)
	assert.Equal(t, "v2", v)
}
