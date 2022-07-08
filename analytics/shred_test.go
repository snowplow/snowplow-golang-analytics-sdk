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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractSchema(t *testing.T) {
	assert := assert.New(t)

	// correct value
	schemaParts, err := extractSchema("iglu:com.acme.data/some_event/jsonschema/15-34-1")
	assert.Nil(err)
	// assert.Equal("iglu:", schemaParts.Protocol)
	assert.Equal("com.acme.data", schemaParts.Vendor)
	assert.Equal("some_event", schemaParts.Name)
	// assert.Equal("jsonschema", schemaParts.Format)
	assert.Equal("15", schemaParts.Model)
	// assert.Equal("-34-1", schemaParts.Revision)

	// invalid schema path
	invalidSchemaParts, err := extractSchema("com.acme.notvalid/invalidschemapath/jsonschema/1.0.0")
	assert.NotNil(err)
	// assert.Zero(invalidSchemaParts.Protocol)
	assert.Zero(invalidSchemaParts.Vendor)
	assert.Zero(invalidSchemaParts.Name)
	// assert.Zero(invalidSchemaParts.Format)
	assert.Zero(invalidSchemaParts.Model)
	// assert.Zero(invalidSchemaParts.Revision)
}

func BenchmarkExtractSchema(b *testing.B) {
	for i := 0; i < b.N; i++ {
		extractSchema("iglu:com.acme.data/some_event/jsonschema/15-34-1")
	}
}

/*

OLD:

cpu: Intel(R) Core(TM) i7-6820HQ CPU @ 2.70GHz
BenchmarkExtractSchema-8       	   86869	     13341 ns/op
BenchmarkInsertUnderscores-8   	 3446166	       357.9 ns/op
BenchmarkFixSchema-8           	   86626	     13806 ns/op
BenchmarkShredContexts-8       	   39648	     31612 ns/op
BenchmarkShredUnstruct-8       	   73705	     17359 ns/op
BenchmarkParseTime-8           	 3561141	       334.0 ns/op
BenchmarkParseString-8         	16929358	        72.46 ns/op
BenchmarkParseInt-8            	24347121	        46.27 ns/op
BenchmarkParseBool-8           	25356945	        45.39 ns/op
BenchmarkParseDouble-8         	10472491	       112.9 ns/op
BenchmarkParseEvent-8          	  546532	      2101 ns/op
BenchmarkMapifyGoodEvent-8     	   12667	     94598 ns/op
BenchmarkToJson-8              	    8798	    119410 ns/op
BenchmarkToJsonWithGeo-8       	    8554	    117259 ns/op
BenchmarkToMap-8               	   12602	     94751 ns/op
BenchmarkToMapWithGeo-8        	   12058	     95363 ns/op
BenchmarkGetValue-8            	   19915	     60175 ns/op
BenchmarkGetContextValue-8     	    5587	    204312 ns/op
BenchmarkGetSubsetMap-8        	   19491	     62871 ns/op
BenchmarkGetSubsetJson-8       	   16533	     73319 ns/op


NEW:

cpu: Intel(R) Core(TM) i7-6820HQ CPU @ 2.70GHz
BenchmarkExtractSchema-8       	 4332684	       272.7 ns/op
BenchmarkInsertUnderscores-8   	 3284797	       389.5 ns/op
BenchmarkFixSchema-8           	 1671493	       694.3 ns/op
BenchmarkShredContexts-8       	  288291	      3972 ns/op
BenchmarkShredUnstruct-8       	  545361	      2054 ns/op
BenchmarkParseTime-8           	 3555187	       333.7 ns/op
BenchmarkParseString-8         	16901078	        68.76 ns/op
BenchmarkParseInt-8            	23274674	        46.61 ns/op
BenchmarkParseBool-8           	25224981	        45.48 ns/op
BenchmarkParseDouble-8         	10314085	       112.0 ns/op
BenchmarkParseEvent-8          	  529095	      2115 ns/op
BenchmarkMapifyGoodEvent-8     	   32546	     36509 ns/op
BenchmarkToJson-8              	   21270	     56620 ns/op
BenchmarkToJsonWithGeo-8       	   20989	     56835 ns/op
BenchmarkToMap-8               	   32090	     41841 ns/op
BenchmarkToMapWithGeo-8        	   32259	     36981 ns/op
BenchmarkGetValue-8            	   65144	     18124 ns/op
BenchmarkGetContextValue-8     	   16526	     72281 ns/op
BenchmarkGetSubsetMap-8        	   62479	     18903 ns/op
BenchmarkGetSubsetJson-8       	   42502	     27752 ns/op
*/

func TestInsertUnderscores(t *testing.T) {
	assert := assert.New(t)

	// camel case
	underscoredCamelCase := insertUnderscores("ThisStringIsCamelCase")
	assert.Equal("This_String_Is_Camel_Case", underscoredCamelCase)

	// abomination
	underscoredMixture := insertUnderscores("this_StringIsAMixture")
	assert.Equal("this_String_Is_A_Mixture", underscoredMixture)
}

func BenchmarkInsertUnderscores(b *testing.B) {
	for i := 0; i < b.N; i++ {
		insertUnderscores("ThisStringIsCamelCase")
	}
}

func TestFixSchema(t *testing.T) {
	assert := assert.New(t)

	// correct value
	fixedSchema, err := fixSchema("unstruct", "iglu:com.acme.data/some_event/jsonschema/15-34-1")
	assert.Nil(err)
	assert.Equal("unstruct_com_acme_data_some_event_15", fixedSchema)

	// invalid schema
	brokenSchema, err := fixSchema("unstruct", "iglu:com.broken.path//jsonschema/1-0-0") // This test fails with new way of parsing the schema.
	assert.NotNil(err)
	assert.Zero(brokenSchema)
}

func BenchmarkFixSchema(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fixSchema("unstruct", "iglu:com.acme.data/some_event/jsonschema/15-34-1")
	}
}

func TestShredContexts(t *testing.T) {
	assert := assert.New(t)

	// correct values
	map1 := map[string]interface{}{"field1": 1.0} // using decimals as the interface value is interpreted as float64
	map2 := map[string]interface{}{"field1": 2.0}
	var expected = []KeyVal{{"contexts_com_acme_test_context_1", []interface{}{map1, map2}}}

	shreddedContexts, err := shredContexts(ctxt)
	assert.Nil(err)
	assert.Equal(expected, shreddedContexts)

	// invalid input
	failedShred, err := shredContexts(invalidCtxt)
	assert.NotNil(err)
	assert.Nil(failedShred)
}

func BenchmarkShredContexts(b *testing.B) {
	for i := 0; i < b.N; i++ {
		shredContexts(ctxt)
	}
}

func TestShredUnstruct(t *testing.T) {
	assert := assert.New(t)

	// correct values
	map1 := map[string]interface{}{"key": "value"}
	expected := []KeyVal{{"unstruct_event_com_snowplowanalytics_snowplow_link_click_1", map1}}

	shreddedUnstruct, err := shredUnstruct(unstruct)
	assert.Nil(err)
	assert.Equal(expected, shreddedUnstruct)

	failedShred, err := shredUnstruct(invalidUnstruct)
	assert.NotNil(err)
	assert.Nil(failedShred)
}

func BenchmarkShredUnstruct(b *testing.B) {
	for i := 0; i < b.N; i++ {
		shredUnstruct(unstruct)
	}
}
