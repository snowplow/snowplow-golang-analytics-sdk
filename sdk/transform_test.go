package sdk

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

	// TODO: Move vars to vars_test.go
	unstructMap := map[string]interface{}{
		"elementClasses": []interface{}{"foreground"},
		"elementId":      "exampleLink",
		"targetUrl":      "http://www.example.com",
	}

	unstructValue, err := GetValue(tsvEvent, "unstruct_event")

	assert.Equal(unstructMap, unstructValue)

	contextsMap := map[string]interface{}{
		"contexts_org_w3_performance_timing_1": []interface{}{
			map[string]interface{}{
				"connectEnd":                 1.415358090183e+12,
				"connectStart":               1.415358090103e+12,
				"domComplete":                0.0,
				"domContentLoadedEventEnd":   1.415358091309e+12,
				"domContentLoadedEventStart": 1.415358090968e+12,
				"domInteractive":             1.415358090886e+12,
				"domLoading":                 1.41535809027e+12,
				"domainLookupEnd":            1.415358090102e+12,
				"domainLookupStart":          1.415358090102e+12,
				"fetchStart":                 1.41535808987e+12,
				"loadEventEnd":               0.0,
				"loadEventStart":             0.0,
				"navigationStart":            1.415358089861e+12,
				"redirectEnd":                0.0,
				"redirectStart":              0.0,
				"requestStart":               1.415358090183e+12,
				"responseEnd":                1.415358090265e+12,
				"responseStart":              1.415358090265e+12,
				"unloadEventEnd":             1.415358090287e+12,
				"unloadEventStart":           1.41535809027e+12,
			},
		},
		"contexts_org_schema_web_page_1": []interface{}{
			map[string]interface{}{
				"author":        "Fred Blundun",
				"breadcrumb":    []interface{}{"blog", "releases"},
				"datePublished": "2014-11-06T00:00:00Z",
				"genre":         "blog",
				"inLanguage":    "en-US",
				"keywords":      []interface{}{"snowplow", "javascript", "tracker", "event"},
			},
		},
	}

	contextsValue, err := GetValue(tsvEvent, "contexts")

	assert.Equal(contextsMap, contextsValue)
}

func TestGetSubsetMap(t *testing.T) {
	assert := assert.New(t)

	subsetMap := map[string]interface{}{
		"app_id":            "angry-birds",
		"br_features_flash": false,
		"br_features_pdf":   true,
		"collector_tstamp":  &tstampValue,
	}

	subsetMapValue, _ := GetSubsetMap(tsvEvent, []string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp"})

	assert.Equal(subsetMap, subsetMapValue)
}

func TestGetSubsetJSON(t *testing.T) {
	assert := assert.New(t)

	subsetJson, _ := json.Marshal(map[string]interface{}{
		"app_id":            "angry-birds",
		"br_features_flash": false,
		"br_features_pdf":   true,
		"collector_tstamp":  &tstampValue,
	})

	subsetJsonValue, _ := GetSubsetJson(tsvEvent, []string{"app_id", "br_features_flash", "br_features_pdf", "collector_tstamp"})

	assert.Equal(subsetJson, subsetJsonValue)
}
