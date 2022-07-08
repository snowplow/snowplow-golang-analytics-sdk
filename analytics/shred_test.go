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
	assert.Zero(invalidSchemaParts.Format)
	assert.Zero(invalidSchemaParts.Model)
	assert.Zero(invalidSchemaParts.Revision)
}

func BenchmarkExtractSchema(b *testing.B) {
	for i := 0; i < b.N; i++ {
		extractSchema("iglu:com.acme.data/some_event/jsonschema/15-34-1")
	}
}

/*

OLD:

cpu: Intel(R) Core(TM) i7-6820HQ CPU @ 2.70GHz
BenchmarkExtractSchema-8       	   88786	     13298 ns/op
BenchmarkInsertUnderscores-8   	 3399079	       350.9 ns/op
BenchmarkFixSchema-8           	   87328	     13561 ns/op
BenchmarkShredContexts-8       	   39430	     30701 ns/op
BenchmarkShredUnstruct-8       	   76174	     15624 ns/op
BenchmarkParseTime-8           	 3615818	       338.0 ns/op
BenchmarkParseString-8         	17033172	        68.20 ns/op
BenchmarkParseInt-8            	25019544	        45.76 ns/op
BenchmarkParseBool-8           	25588480	        45.65 ns/op
BenchmarkParseDouble-8         	10564029	       111.8 ns/op
BenchmarkParseEvent-8          	  547250	      2098 ns/op
BenchmarkMapifyGoodEvent-8     	   12790	     93939 ns/op
BenchmarkToJson-8              	    9728	    117682 ns/op
BenchmarkToJsonWithGeo-8       	    9694	    116504 ns/op
BenchmarkToMap-8               	   12445	     97730 ns/op
BenchmarkToMapWithGeo-8        	   12766	     94113 ns/op
BenchmarkGetValue-8            	   20223	     59082 ns/op
BenchmarkGetContextValue-8     	   14071	     85239 ns/op		<--- This skips the method in question
BenchmarkGetSubsetMap-8        	   19278	     61060 ns/op
BenchmarkGetSubsetJson-8       	   16923	     70643 ns/op


NEW:

cpu: Intel(R) Core(TM) i7-6820HQ CPU @ 2.70GHz
BenchmarkExtractSchema-8        	  685042	      1472 ns/op			<--- ~10x faster.
BenchmarkInsertUnderscores-8    	 3458832	       344.9 ns/op
BenchmarkFixSchema-8            	  573596	      2016 ns/op			<--- ~6x faster
BenchmarkShredContexts-8        	  161252	      6994 ns/op			<--- ~5x faster
BenchmarkShredUnstruct-8        	  313735	      3602 ns/op			<--- ~5x faster
BenchmarkParseTime-8            	 3613819	       330.8 ns/op
BenchmarkParseString-8          	16938556	        67.77 ns/op
BenchmarkParseInt-8             	24495948	        46.64 ns/op
BenchmarkParseBool-8            	25445799	        45.06 ns/op
BenchmarkParseDouble-8          	10555526	       111.1 ns/op
BenchmarkParseEvent-8           	  555231	      2087 ns/op
BenchmarkMapifyGoodEvent-8      	   27151	     48086 ns/op			<--- ~2x faster
BenchmarkToJson-8               	   18259	     64889 ns/op			<--- almost ~2x faster
BenchmarkToJsonWithGeo-8        	   18312	     65923 ns/op			<--- almost ~2x faster
BenchmarkToMap-8                	   27051	     44321 ns/op			<--- ~2x faster
BenchmarkToMapWithGeo-8         	   26786	     44458 ns/op			<--- ~2x faster
BenchmarkGetValue-8             	   50256	     23802 ns/op			<--- ~2x faster
BenchmarkGetContextValue-8      	   14200	     84536 ns/op		<----- Borh old and new are faster now
BenchmarkGetContextValueOld-8   	   13342	     89937 ns/op		<----- New way of doing this function looks no better, but both improved vastly with improved extractSchema.
BenchmarkGetSubsetMap-8         	   47990	     24658 ns/op			<--- ~3x faster
BenchmarkGetSubsetJson-8        	   35016	     33774 ns/op			<--- ~2x faster
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
