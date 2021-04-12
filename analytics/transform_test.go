package analytics

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseNullableTime(t *testing.T) {
	assert := assert.New(t)

	tstamp, err := parseNullableTime("2013-11-26 00:03:57.885")
	notTstamp, err2 := parseNullableTime("this is not a tstamp")
	zeroValue, err3 := parseNullableTime("")

	assert.Nil(err)
	assert.NotNil(tstamp)
	assert.NotZero(tstamp)
	assert.Equal(&tstampValue, tstamp)

	assert.NotNil(err2)
	assert.Nil(notTstamp)

	assert.Nil(zeroValue)
	assert.NotNil(err3)
}

func BenchmarkParseNullableTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseNullableTime("2021-04-07 12:01:01.999")
	}
}

func TestParseTime(t *testing.T) {
	assert := assert.New(t)

	tstamp, err := parseTime("tstampKey", "2013-11-26 00:03:57.885")
	notTstamp, err2 := parseTime("tstampKey", "not a tstamp")
	zeroValue, err3 := parseTime("tstampKey", "")

	assert.Nil(err)
	assert.NotNil(tstamp)
	assert.NotZero(tstamp)
	assert.Equal([]KeyVal{KeyVal{"tstampKey", &tstampValue}}, tstamp)

	assert.NotNil(err2)
	assert.Nil(notTstamp)

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

	parsedString, err := parseString("stringKey", "stringValue")
	zeroValue, err2 := parseString("stringKey", "")

	assert.Nil(err)
	assert.Equal([]KeyVal{KeyVal{"stringKey", "stringValue"}}, parsedString)

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

	parsedInt, err := parseInt("intKey", "123")
	notInt, err2 := parseInt("intKey", "notAnInt")
	zeroValue, err3 := parseInt("intKey", "")

	assert.Nil(err)
	assert.Equal([]KeyVal{KeyVal{"intKey", 123}}, parsedInt)

	assert.Nil(notInt)
	assert.NotNil(err2)

	assert.NotNil(err3)
	assert.Nil(zeroValue)
}

func BenchmarkParseInt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseInt("intKey", "123")
	}
}

func TestParseBool(t *testing.T) {
	assert := assert.New(t)

	parsedBool, err := parseBool("boolKey", "1")
	notBool, err2 := parseBool("boolKey", "notABool")
	zeroValue, err3 := parseBool("boolKey", "")

	assert.Nil(err)
	assert.Equal([]KeyVal{KeyVal{"boolKey", true}}, parsedBool)

	assert.Nil(notBool)
	assert.NotNil(err2)

	assert.NotNil(err3)
	assert.Nil(zeroValue)
}

func BenchmarkParseBool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseBool("boolKey", "1")
	}
}

func TestParseDouble(t *testing.T) {
	assert := assert.New(t)

	parsedDouble, err := parseDouble("doubleKey", "1.23")
	notDouble, err2 := parseDouble("doubleKey", "notADouble")
	zeroValue, err3 := parseDouble("doubleKey", "")

	assert.Nil(err)
	assert.Equal([]KeyVal{KeyVal{"doubleKey", 1.23}}, parsedDouble)

	assert.Nil(notDouble)
	assert.NotNil(err2)

	assert.NotNil(err3)
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

	mapifiedEvent, err := mapifyGoodEvent(fullEvent, enrichedEventFieldTypes, true)
	failedMapify, err2 := mapifyGoodEvent([]string{"one", "two"}, enrichedEventFieldTypes, true)

	assert.Nil(err)
	assert.Equal(eventMap, mapifiedEvent)

	assert.NotNil(err2)
	assert.Nil(failedMapify)
}

func BenchmarkMapifyGoodEvent(b *testing.B) {
	for i := 0; i < b.N; i++ {
		mapifyGoodEvent(fullEvent, enrichedEventFieldTypes, true)
	}
}

func TestTransformToJson(t *testing.T) {
	assert := assert.New(t)

	jsonEvent, err := json.Marshal(eventMap)
	if err != nil {
	}

	jsonifiedEvent, err := TransformToJson(tsvEvent)
	failedJsonify, err2 := TransformToJson("\t\t\t")

	assert.Nil(err)
	assert.Equal(jsonEvent, jsonifiedEvent)

	assert.NotNil(err2)
	assert.Nil(failedJsonify)
}

func BenchmarkTransformToJson(b *testing.B) {
	for i := 0; i < b.N; i++ {
		TransformToJson(tsvEvent)
	}
}

func TestTransformToMap(t *testing.T) {
	assert := assert.New(t)

	mapifiedEvent, err := TransformToMap(tsvEvent)
	failedMapify, err2 := TransformToMap("\t\t\t")

	assert.Nil(err)
	assert.Equal(eventMap, mapifiedEvent)

	assert.NotNil(err2)
	assert.Nil(failedMapify)
}

func BenchmarkTransformToMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		TransformToMap(tsvEvent)
	}
}

func TestGetValue(t *testing.T) {
	assert := assert.New(t)

	appId, err := GetValue(tsvEvent, "app_id")

	assert.Nil(err)
	assert.Equal("angry-birds", appId)

	unstructValue, err := GetValue(tsvEvent, "unstruct_event")

	assert.Equal(unstructMap, unstructValue)

	contextsValue, err := GetValue(tsvEvent, "contexts")

	assert.Equal(contextsMap, contextsValue)

	failureValue, err := GetValue(tsvEvent, "not_a_field")

	assert.Nil(failureValue)
	assert.NotNil(err)
}


func BenchmarkGetValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GetValue(tsvEvent, "app_id")
		GetValue(tsvEvent, "contexts")
		GetValue(tsvEvent, "unstruct_event") // Calling it three times to ensure benchmark includes both simple and complex cases
	}
}

func TestGetSubsetMap(t *testing.T) {
	assert := assert.New(t)

	// TODO: Include contexts and unstruct in this, those should be part of any unit test.
	subsetMapValue, err := GetSubsetMap(tsvEvent, []string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp"})

	assert.Equal(subsetMap, subsetMapValue)
	assert.Nil(err)

	failureMap, err := GetSubsetMap(tsvEvent, []string{"not_a_field", "app_id", "br_features_flash", "br_features_pdf", "collector_tstamp"})

	assert.Nil(failureMap)
	assert.NotNil(err)
}

func BenchmarkGetSubsetMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GetSubsetMap(tsvEvent, []string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp", "contexts", "unstruct_event"})
	}
}

func TestGetSubsetJSON(t *testing.T) {
	assert := assert.New(t)

	subsetJsonValue, err := GetSubsetJson(tsvEvent, []string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp"})

	assert.Equal(subsetJson, subsetJsonValue)
	assert.Nil(err)

	failureJson, err := GetSubsetJson(tsvEvent, []string{"not_a_field", "app_id", "br_features_flash", "br_features_pdf", "collector_tstamp"})

	assert.Nil(failureJson)
	assert.NotNil(err)
}

func BenchmarkGetSubsetJson(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GetSubsetJson(tsvEvent, []string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp", "contexts", "unstruct_event"})
	}
}
