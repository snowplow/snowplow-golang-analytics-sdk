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
	stdJson "encoding/json" // Using the std JSON package for expected values
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTime(t *testing.T) {
	assert := assert.New(t)

	// correct value
	tstamp, err := parseTime("tstampKey", "2013-11-26 00:03:57.885")

	assert.Nil(err)
	assert.Equal([]KeyVal{{"tstampKey", tstampValue}}, tstamp)

	// incorrect format
	notTstamp, err := parseTime("tstampKey", "not a tstamp")

	assert.NotNil(err)
	assert.Nil(notTstamp)

	// empty input
	emptyInput, err := parseTime("tstampKey", "")

	assert.Nil(emptyInput)
	assert.NotNil(err)
}

func BenchmarkParseTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseTime("tstampKey", "2021-04-07 12:01:01.999")
	}
}

func TestParseString(t *testing.T) {
	assert := assert.New(t)

	// correct value
	parsedString, err := parseString("stringKey", "stringValue")

	assert.Nil(err)
	assert.Equal([]KeyVal{{"stringKey", "stringValue"}}, parsedString)

	// Zero value
	zeroValue, err2 := parseString("stringKey", "")

	assert.NotNil(err2)
	assert.Nil(zeroValue)
}

func BenchmarkParseString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseString("stringKey", "stringValue")
	}
}

func TestParseInt(t *testing.T) {
	assert := assert.New(t)

	// correct value
	parsedInt, err := parseInt("intKey", "123")

	assert.Nil(err)
	assert.Equal([]KeyVal{{"intKey", 123}}, parsedInt)

	// Incorrect format
	notInt, err := parseInt("intKey", "notAnInt")

	assert.NotNil(err)
	assert.Nil(notInt)

	// zero value
	zeroValue, err := parseInt("intKey", "")

	assert.NotNil(err)
	assert.Nil(zeroValue)
}

func BenchmarkParseInt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseInt("intKey", "123")
	}
}

func TestParseBool(t *testing.T) {
	assert := assert.New(t)

	// correct value
	parsedBool, err := parseBool("boolKey", "1")

	assert.Nil(err)
	assert.Equal([]KeyVal{{"boolKey", true}}, parsedBool)

	// incorrect format
	notBool, err := parseBool("boolKey", "notABool")

	assert.NotNil(err)
	assert.Nil(notBool)

	// zero value
	zeroValue, err := parseBool("boolKey", "")

	assert.NotNil(err)
	assert.Nil(zeroValue)
}

func BenchmarkParseBool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseBool("boolKey", "1")
	}
}

func TestParseDouble(t *testing.T) {
	assert := assert.New(t)

	// correct value
	parsedDouble, err := parseDouble("doubleKey", "1.23")
	assert.Nil(err)
	assert.Equal([]KeyVal{{"doubleKey", 1.23}}, parsedDouble)

	// incorrect format
	notDouble, err := parseDouble("doubleKey", "notADouble")
	assert.Nil(notDouble)
	assert.NotNil(err)

	// zero value
	zeroValue, err := parseDouble("doubleKey", "")
	assert.NotNil(err)
	assert.Nil(zeroValue)
}

func BenchmarkParseDouble(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseDouble("doubleKey", "1234.234567")
	}
}

// parseContexts and parseUnstruct tests covered in shred_test.go.

func TestParseEvent(t *testing.T) {
	assert := assert.New(t)

	// correct values
	parsedEvent, err := ParseEvent(tsvEvent)
	assert.Nil(err)
	assert.Equal(fullEvent, parsedEvent)

	// incorrect input
	brokenTsvString, err := ParseEvent("\t\t\t")
	assert.NotNil(err)
	assert.Nil(brokenTsvString)
}

func BenchmarkParseEvent(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ParseEvent(tsvEvent)
	}
}

func TestMapifyGoodEvent(t *testing.T) {
	assert := assert.New(t)

	// correct value with geo
	mapifiedEventWithGeo, err := fullEvent.mapifyGoodEvent(enrichedEventFieldTypes, true)
	assert.Nil(err)
	assert.Equal(eventMapWithGeo, mapifiedEventWithGeo)

	// correct value without geo
	mapifiedEventWithoutGeo, err := fullEvent.mapifyGoodEvent(enrichedEventFieldTypes, false)
	assert.Nil(err)
	assert.Equal(eventMapWithoutGeo, mapifiedEventWithoutGeo)

	// incorrect input length
	failedMapify, err := ParsedEvent([]string{"one", "two"}).mapifyGoodEvent(enrichedEventFieldTypes, true)
	assert.NotNil(err)
	assert.Nil(failedMapify)
}

func BenchmarkMapifyGoodEvent(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fullEvent.mapifyGoodEvent(enrichedEventFieldTypes, true)
	}
}

func TestToJson(t *testing.T) {
	assert := assert.New(t)

	// correct value
	jsonEvent, err := stdJson.Marshal(eventMapWithoutGeo)
	if err != nil {
	}

	jsonifiedEvent, err := fullEvent.ToJson()
	assert.Nil(err)
	assert.Equal(jsonEvent, jsonifiedEvent)

	// incorrect input
	failedJsonify, err := ParsedEvent([]string{"one", "two"}).ToJson()
	assert.NotNil(err)
	assert.Nil(failedJsonify)
}

func BenchmarkToJson(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fullEvent.ToJson()
	}
}

func TestToJsonWithGeo(t *testing.T) {
	assert := assert.New(t)

	// correct value
	jsonEvent, err := stdJson.Marshal(eventMapWithGeo)
	if err != nil {
	}

	jsonifiedEvent, err := fullEvent.ToJsonWithGeo()
	assert.Nil(err)
	assert.Equal(jsonEvent, jsonifiedEvent)

	// incorrect input
	failedJsonify, err := ParsedEvent([]string{"one", "two"}).ToJsonWithGeo()
	assert.NotNil(err)
	assert.Nil(failedJsonify)
}

func BenchmarkToJsonWithGeo(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fullEvent.ToJsonWithGeo()
	}
}

func TestToMap(t *testing.T) {
	assert := assert.New(t)

	// correct value
	mapifiedEvent, err := fullEvent.ToMap()
	assert.Nil(err)
	assert.Equal(eventMapWithoutGeo, mapifiedEvent)

	// incorrect input
	failedMapify, err := ParsedEvent([]string{"one", "two"}).ToMap()
	assert.NotNil(err)
	assert.Nil(failedMapify)
}

func BenchmarkToMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fullEvent.ToMap()
	}
}

func TestToMapWithGeo(t *testing.T) {
	assert := assert.New(t)

	// correct value
	mapifiedEvent, err := fullEvent.ToMapWithGeo()
	assert.Nil(err)
	assert.Equal(eventMapWithGeo, mapifiedEvent)

	// incorrect input
	failedMapify, err := ParsedEvent([]string{"one", "two"}).ToMapWithGeo()
	assert.NotNil(err)
	assert.Nil(failedMapify)
}

func BenchmarkToMapWithGeo(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fullEvent.ToMapWithGeo()
	}
}

func TestGetValue(t *testing.T) {
	assert := assert.New(t)

	// correct value simple field
	appId, err := fullEvent.GetValue("app_id")
	assert.Nil(err)
	assert.Equal("angry-birds", appId)

	// correct value unstruct field
	unstructValue, err := fullEvent.GetValue("unstruct_event")
	assert.Nil(err)
	assert.Equal(unstructMap, unstructValue)

	// correct value contexts
	contextsValue, err := fullEvent.GetValue("contexts")
	assert.Nil(err)
	assert.Equal(contextsMap, contextsValue)

	// incorrect field name
	failureValue, err := fullEvent.GetValue("not_a_field")
	assert.Nil(failureValue)
	assert.NotNil(err)

	// empty value
	emptyValue, err := fullEvent.GetValue("ti_name")
	assert.Nil(emptyValue)
	assert.NotNil(err)
}

func BenchmarkGetValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fullEvent.GetValue("app_id")
		fullEvent.GetValue("contexts")
		fullEvent.GetValue("unstruct_event") // Calling it three times to ensure benchmark includes both simple and complex data
	}
}

func TestGetSubsetMap(t *testing.T) {
	assert := assert.New(t)

	// correct values
	subsetMapValue, err := fullEvent.GetSubsetMap([]string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp", "unstruct_event", "contexts", "derived_contexts"}...)
	assert.Equal(subsetMap, subsetMapValue)
	assert.Nil(err)

	// correct values passing multiple string args
	subsetMapValue2, err := fullEvent.GetSubsetMap("app_id", "br_features_flash", "br_features_pdf", "collector_tstamp", "unstruct_event", "contexts", "derived_contexts")
	assert.Equal(subsetMap, subsetMapValue2)
	assert.Nil(err)

	// incorrect field name
	failureMap, err := fullEvent.GetSubsetMap([]string{"not_a_field", "app_id", "br_features_flash", "br_features_pdf", "collector_tstamp"}...)
	assert.Nil(failureMap)
	assert.NotNil(err)

	// empty value
	emptyValue, err := fullEvent.GetSubsetMap("ti_name")
	assert.Equal(make(map[string]interface{}), emptyValue)
	assert.Nil(err)
}

func BenchmarkGetSubsetMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fullEvent.GetSubsetMap([]string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp", "contexts", "unstruct_event"}...)
	}
}

func TestGetSubsetJSON(t *testing.T) {
	assert := assert.New(t)

	// correct value
	subsetJsonValue, err := fullEvent.GetSubsetJson([]string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp", "unstruct_event", "contexts", "derived_contexts"}...)
	assert.Equal(subsetJson, subsetJsonValue)
	assert.Nil(err)

	// correct values passing multiple string args
	subsetJsonValue2, err := fullEvent.GetSubsetJson("app_id", "br_features_flash", "br_features_pdf", "collector_tstamp", "unstruct_event", "contexts", "derived_contexts")
	assert.Equal(subsetJson, subsetJsonValue2)
	assert.Nil(err)

	// incorrect field name
	failureJson, err := fullEvent.GetSubsetJson([]string{"not_a_field", "app_id", "br_features_flash", "br_features_pdf", "collector_tstamp"}...)
	assert.Nil(failureJson)
	assert.NotNil(err)

	// empty value
	emptyJson, _ := stdJson.Marshal(make(map[string]interface{}))
	emptyValue, err := fullEvent.GetSubsetJson("ti_name")
	assert.Equal(emptyJson, emptyValue)
	assert.Nil(err)
}

func BenchmarkGetSubsetJson(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fullEvent.GetSubsetJson([]string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp", "contexts", "unstruct_event"}...)
	}
}
