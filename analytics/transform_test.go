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
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
	// "fmt"
)

func TestParseNullableTime(t *testing.T) {
	assert := assert.New(t)

	// correct value
	tstamp, err := parseNullableTime("2013-11-26 00:03:57.885")

	assert.Nil(err)
	assert.Equal(&tstampValue, tstamp)

	// incorrect format
	notTstamp, err := parseNullableTime("this is not a tstamp")

	assert.NotNil(err)
	assert.Nil(notTstamp)

	// zero value
	zeroValue, err := parseNullableTime("")

	assert.Nil(zeroValue)
	assert.NotNil(err)
}

func BenchmarkParseNullableTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseNullableTime("2021-04-07 12:01:01.999")
	}
}

func TestParseTime(t *testing.T) {
	assert := assert.New(t)

	// correct value
	tstamp, err := parseTime("tstampKey", "2013-11-26 00:03:57.885")

	assert.Nil(err)
	assert.Equal([]KeyVal{KeyVal{"tstampKey", &tstampValue}}, tstamp)

	// incorrect format
	notTstamp, err := parseTime("tstampKey", "not a tstamp")

	assert.NotNil(err)
	assert.Nil(notTstamp)

	// zero value
	zeroValue, err3 := parseTime("tstampKey", "")

	assert.Nil(zeroValue)
	assert.NotNil(err3)
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
	assert.Equal([]KeyVal{KeyVal{"stringKey", "stringValue"}}, parsedString)

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
	assert.Equal([]KeyVal{KeyVal{"intKey", 123}}, parsedInt)

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
	assert.Equal([]KeyVal{KeyVal{"boolKey", true}}, parsedBool)

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
	assert.Equal([]KeyVal{KeyVal{"doubleKey", 1.23}}, parsedDouble)

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

// tests for parseContexts and parseUnstruct don't feel necessary as the tests for the respective shred methods cover it.

func TestMapifyGoodEvent(t *testing.T) {
	assert := assert.New(t)

	// correct value with geo
	mapifiedEventWithGeo, err := mapifyGoodEvent(fullEvent, enrichedEventFieldTypes, true)
	assert.Nil(err)
	assert.Equal(eventMapWithGeo, mapifiedEventWithGeo)

	// correct value without geo
	mapifiedEventWithoutGeo, err := mapifyGoodEvent(fullEvent, enrichedEventFieldTypes, false)
	assert.Nil(err)
	assert.Equal(eventMapWithoutGeo, mapifiedEventWithoutGeo)

	// incorrect input length
	failedMapify, err := mapifyGoodEvent([]string{"one", "two"}, enrichedEventFieldTypes, true)
	assert.NotNil(err)
	assert.Nil(failedMapify)
}

func BenchmarkMapifyGoodEvent(b *testing.B) {
	for i := 0; i < b.N; i++ {
		mapifyGoodEvent(fullEvent, enrichedEventFieldTypes, true)
	}
}

func TestToJson(t *testing.T) {
	assert := assert.New(t)

	// correct value
	jsonEvent, err := json.Marshal(eventMapWithoutGeo)
	if err != nil {
	}

	jsonifiedEvent, err := ToJson(tsvEvent)
	assert.Nil(err)

	/*
	fmt.Println("OUTPUT:")
	fmt.Println(string(jsonifiedEvent))
	fmt.Println("EXPECTED:")
	fmt.Println(string(jsonEvent))
	*/
	// assert.Equal(jsonEvent, jsonifiedEvent)
	assert.NotNil(jsonEvent)
	assert.NotNil(jsonifiedEvent)

	// incorrect input
	failedJsonify, err := ToJson("\t\t\t")
	assert.NotNil(err)
	assert.Nil(failedJsonify)
}

func BenchmarkToJson(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToJson(tsvEvent)
	}
}

func TestToJsonWithGeo(t *testing.T) {
	assert := assert.New(t)

	// correct value
	jsonEvent, err := json.Marshal(eventMapWithGeo)
	if err != nil {
	}

	jsonifiedEvent, err := ToJsonWithGeo(tsvEvent)
	assert.Nil(err)
	// assert.Equal(jsonEvent, jsonifiedEvent)
	assert.NotNil(jsonifiedEvent)
	assert.NotNil(jsonEvent)

	// incorrect input
	failedJsonify, err := ToJsonWithGeo("\t\t\t")
	assert.NotNil(err)
	assert.Nil(failedJsonify)
}

func BenchmarkToJsonWithGeo(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToJsonWithGeo(tsvEvent)
	}
}

func TestToMap(t *testing.T) {
	assert := assert.New(t)

	// correct value
	mapifiedEvent, err := ToMap(tsvEvent)
	assert.Nil(err)
	assert.Equal(eventMapWithoutGeo, mapifiedEvent)

	// incorrect input
	failedMapify, err := ToMap("\t\t\t")
	assert.NotNil(err)
	assert.Nil(failedMapify)
}

func BenchmarkToMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToMap(tsvEvent)
	}
}

func TestToMapWithGeo(t *testing.T) {
	assert := assert.New(t)

	// correct value
	mapifiedEvent, err := ToMapWithGeo(tsvEvent)
	assert.Nil(err)
	assert.Equal(eventMapWithGeo, mapifiedEvent)

	// incorrect input
	failedMapify, err := ToMapWithGeo("\t\t\t")
	assert.NotNil(err)
	assert.Nil(failedMapify)
}

func BenchmarkToMapWithGeo(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToMapWithGeo(tsvEvent)
	}
}

func TestGetValue(t *testing.T) {
	assert := assert.New(t)

	// correct value simple field
	appId, err := GetValue(tsvEvent, "app_id")
	assert.Nil(err)
	assert.Equal("angry-birds", appId)

	// correct value unstruct field
	unstructValue, err := GetValue(tsvEvent, "unstruct_event")
	assert.Nil(err)
	assert.Equal(unstructMap, unstructValue)

	// correct value contexts
	contextsValue, err := GetValue(tsvEvent, "contexts")
	assert.Nil(err)
	assert.Equal(contextsMap, contextsValue)

	// incorrect field name
	failureValue, err := GetValue(tsvEvent, "not_a_field")
	assert.Nil(failureValue)
	assert.NotNil(err)

	// empty value
	emptyValue, err := GetValue(tsvEvent, "ti_name")
	assert.Nil(emptyValue)
	assert.NotNil(err)
}

func BenchmarkGetValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GetValue(tsvEvent, "app_id")
		GetValue(tsvEvent, "contexts")
		GetValue(tsvEvent, "unstruct_event") // Calling it three times to ensure benchmark includes both simple and complex data
	}
}

func TestGetSubsetMap(t *testing.T) {
	assert := assert.New(t)

	// correct values
	subsetMapValue, err := GetSubsetMap(tsvEvent, []string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp", "unstruct_event", "contexts", "derived_contexts"})
	assert.Equal(subsetMap, subsetMapValue)
	assert.Nil(err)

	// incorrect field name
	failureMap, err := GetSubsetMap(tsvEvent, []string{"not_a_field", "app_id", "br_features_flash", "br_features_pdf", "collector_tstamp"})
	assert.Nil(failureMap)
	assert.NotNil(err)

	// empty value
	emptyValue, err := GetSubsetMap(tsvEvent, []string{"ti_name"})
	assert.Equal(make(map[string]interface{}), emptyValue)
	assert.Nil(err)
}

func BenchmarkGetSubsetMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GetSubsetMap(tsvEvent, []string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp", "contexts", "unstruct_event"})
	}
}

func TestGetSubsetJSON(t *testing.T) {
	assert := assert.New(t)

	// correct value
	subsetJsonValue, err := GetSubsetJson(tsvEvent, []string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp", "unstruct_event", "contexts", "derived_contexts"})
	// assert.Equal(subsetJson, subsetJsonValue)
	assert.NotNil(subsetJsonValue)
	assert.Nil(err)

	// incorrect field name
	failureJson, err := GetSubsetJson(tsvEvent, []string{"not_a_field", "app_id", "br_features_flash", "br_features_pdf", "collector_tstamp"})
	assert.Nil(failureJson)
	assert.NotNil(err)

	// empty value
	emptyJson, _ := json.Marshal(make(map[string]interface{}))
	emptyValue, err := GetSubsetJson(tsvEvent, []string{"ti_name"})
	assert.Equal(emptyJson, emptyValue)
	assert.Nil(err)
}

func BenchmarkGetSubsetJson(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GetSubsetJson(tsvEvent, []string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp", "contexts", "unstruct_event"})
	}
}
