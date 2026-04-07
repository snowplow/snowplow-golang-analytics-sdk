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

	"github.com/stretchr/testify/assert"
)

func TestExtractSchema(t *testing.T) {
	assert := assert.New(t)

	// correct value
	schemaParts, err := extractSchema("iglu:com.acme.data/some_event/jsonschema/15-34-1")
	assert.Nil(err)
	assert.Equal("iglu:", schemaParts.Protocol)
	assert.Equal("com.acme.data", schemaParts.Vendor)
	assert.Equal("some_event", schemaParts.Name)
	assert.Equal("jsonschema", schemaParts.Format)
	assert.Equal("15", schemaParts.Model)
	assert.Equal("-34-1", schemaParts.Revision)

	// invalid schema path
	invalidSchemaParts, err := extractSchema("com.acme.notvalid/invalidschemapath/jsonschema/1.0.0")
	assert.NotNil(err)
	assert.Zero(invalidSchemaParts.Protocol)
	assert.Zero(invalidSchemaParts.Vendor)
	assert.Zero(invalidSchemaParts.Name)
	assert.Zero(invalidSchemaParts.Format)
	assert.Zero(invalidSchemaParts.Model)
	assert.Zero(invalidSchemaParts.Revision)
}

func BenchmarkExtractSchema(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		extractSchema("iglu:com.acme.data/some_event/jsonschema/15-34-1")
	}
}

func TestInsertUnderscores(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "camel case",
			input:    "ThisStringIsCamelCase",
			expected: "This_String_Is_Camel_Case",
		},
		{
			name:     "mixture with dash",
			input:    "this_String-IsAMixture",
			expected: "this_String_Is_A_Mixture",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "single character",
			input:    "A",
			expected: "A",
		},
		{
			name:     "no uppercase",
			input:    "alllowercase",
			expected: "alllowercase",
		},
		{
			name:     "multi-byte UTF-8",
			input:    "caféLatte",
			expected: "café_Latte",
		},
		{
			name:     "consecutive uppercase after multi-byte",
			input:    "überCoolThing",
			expected: "über_Cool_Thing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, insertUnderscores(tt.input))
		})
	}
}

func BenchmarkInsertUnderscores(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		insertUnderscores("ThisStringIsCamelCase")
	}
}

func TestFixSchema(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		schemaUri string
		expected  string
		wantErr   bool
	}{
		{
			name:      "underscore in name",
			prefix:    "unstruct",
			schemaUri: "iglu:com.acme.data/some_event/jsonschema/15-34-1",
			expected:  "unstruct_com_acme_data_some_event_15",
		},
		{
			name:      "dash in name",
			prefix:    "unstruct",
			schemaUri: "iglu:com.acme.data/some-event/jsonschema/15-34-1",
			expected:  "unstruct_com_acme_data_some_event_15",
		},
		{
			name:      "invalid schema",
			prefix:    "unstruct",
			schemaUri: "iglu:com.broken.path//jsonschema/1-0-0",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fixSchema(tt.prefix, tt.schemaUri)
			if tt.wantErr {
				assert.NotNil(t, err)
				assert.Zero(t, result)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func BenchmarkFixSchema(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		fixSchema("unstruct", "iglu:com.acme.data/some_event/jsonschema/15-34-1")
	}
}

func TestShredContexts(t *testing.T) {
	assert := assert.New(t)

	// correct values
	map1 := map[string]any{"field1": 1.0} // using decimals as the interface value is interpreted as float64
	map2 := map[string]any{"field1": 2.0}
	var expected = []KeyVal{{"contexts_com_acme_test_context_1", []any{map1, map2}}}

	shreddedContexts, err := shredContexts(ctxt)
	assert.Nil(err)
	assert.Equal(expected, shreddedContexts)

	// invalid input
	failedShred, err := shredContexts(invalidCtxt)
	assert.NotNil(err)
	assert.Nil(failedShred)
}

func BenchmarkShredContexts(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		shredContexts(ctxt)
	}
}

func TestShredUnstruct(t *testing.T) {
	assert := assert.New(t)

	// correct values
	map1 := map[string]any{"key": "value"}
	expected := []KeyVal{{"unstruct_event_com_snowplowanalytics_snowplow_link_click_1", map1}}

	shreddedUnstruct, err := shredUnstruct(unstruct)
	assert.Nil(err)
	assert.Equal(expected, shreddedUnstruct)

	failedShred, err := shredUnstruct(invalidUnstruct)
	assert.NotNil(err)
	assert.Nil(failedShred)
}

func BenchmarkShredUnstruct(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		shredUnstruct(unstruct)
	}
}

func BenchmarkFixSchemaRepeated(b *testing.B) {
	b.ReportAllocs()
	uri := "iglu:com.acme.data/some_event/jsonschema/15-34-1"
	for b.Loop() {
		fixSchema("unstruct", uri)
	}
}

func BenchmarkFixSchemaUnique(b *testing.B) {
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		uri := "iglu:com.test/event_" + string(rune(i)) + "/jsonschema/1-0-0"
		fixSchema("unstruct", uri)
	}
}

func BenchmarkFixSchemaParallel(b *testing.B) {
	uri := "iglu:com.acme.data/some_event/jsonschema/15-34-1"
	fixSchema("unstruct", uri)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fixSchema("unstruct", uri)
		}
	})
}

func BenchmarkFixSchemaParallel10(b *testing.B) {
	uris := make([]string, 10)
	for i := range uris {
		uris[i] = fmt.Sprintf("iglu:com.acme.data/event_%d/jsonschema/1-0-0", i)
		fixSchema("contexts", uris[i])
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			fixSchema("contexts", uris[i%10])
			i++
		}
	})
}

func BenchmarkInsertUnderscoresLong(b *testing.B) {
	b.ReportAllocs()
	longString := "ThisIsAReallyLongCamelCaseStringWithManyWordsToTestPerformance"
	for b.Loop() {
		insertUnderscores(longString)
	}
}
