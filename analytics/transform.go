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
	"encoding/json" // TODO: Look into faster options: https://github.com/json-iterator/go-benchmark https://github.com/buger/jsonparser
	"fmt"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"time"
)

type KeyVal struct {
	Key   string
	Value interface{}
}

type ValueParser func(string, string) ([]KeyVal, error)

type KeyFunctionPair struct {
	Key           string
	ParseFunction ValueParser
}

// Using a pointer as one can't return a nil value for time.Time, only a zero value - which might be prone to issues in usage of the sdk.
// Unsure if this is the best decision
func parseNullableTime(timeString string) (*time.Time, error) {
	timeLayout := "2006-01-02 15:04:05.999"
	res, err := time.Parse(timeLayout, timeString)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Error parsing value '%s' to timestamp", timeString))
	}
	if time.Time.IsZero(res) {
		return nil, errors.New(fmt.Sprintf("Timestamp string '%s' resulted in zero-value timestamp", timeString))
	} else {
		return &res, nil
	}
}

func parseTime(key string, value string) ([]KeyVal, error) {
	if value == "" {
		return nil, errors.Wrap(errors.New("Null string found"), fmt.Sprintf("Error parsing key %s", key))
	}
	out, err := parseNullableTime(value)
	if err != nil {
		return nil, errors.Wrap(err, key)
	}
	return []KeyVal{KeyVal{key, out}}, err
}

func parseString(key string, value string) ([]KeyVal, error) {
	if value == "" {
		return nil, errors.Wrap(errors.New("Null string found"), fmt.Sprintf("Error parsing key %s", key))
	}
	return []KeyVal{KeyVal{key, value}}, nil
}

func parseInt(key string, value string) ([]KeyVal, error) {
	if value == "" {
		return nil, errors.Wrap(errors.New("Null string found"), fmt.Sprintf("Error parsing key %s", key))
	}
	intValue, err := strconv.Atoi(value)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Error parsing key '%s' to integer", key))
	}
	return []KeyVal{KeyVal{key, intValue}}, err
}

func parseBool(key string, value string) ([]KeyVal, error) {
	if value == "" {
		return nil, errors.Wrap(errors.New("Null string found"), fmt.Sprintf("Error parsing key %s", key))
	}
	boolValue, err := strconv.ParseBool(value)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Error parsing key '%s' to boolean", key))
	}
	return []KeyVal{KeyVal{key, boolValue}}, err
}

func parseDouble(key string, value string) ([]KeyVal, error) {
	if value == "" {
		return nil, errors.Wrap(errors.New("Null string found"), fmt.Sprintf("Error parsing key %s", key))
	}
	doubleValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Error parsing key '%s' to double", key))
	}
	return []KeyVal{KeyVal{key, doubleValue}}, err
}

func parseContexts(key string, value string) ([]KeyVal, error) {
	if value == "" {
		return nil, errors.Wrap(errors.New("Null string found"), fmt.Sprintf("Error parsing key %s", key))
	}
	return shredContexts(value)
}

func parseUnstruct(key string, value string) ([]KeyVal, error) {
	if value == "" {
		return nil, errors.Wrap(errors.New("Null string found"), fmt.Sprintf("Error parsing key %s", key))
	}
	return shredUnstruct(value)
}

func mapifyGoodEvent(event []string, knownFields [131]KeyFunctionPair, addGeolocationData bool) (map[string]interface{}, error) {
	if len(event) != len(knownFields) {
		return nil, errors.New("Cannot transform event - wrong number of fields")
	} else {
		output := make(map[string]interface{})
		if addGeolocationData && event[latitudeIndex] != "" && event[longitudeIndex] != "" {
			output["geo_location"] = event[latitudeIndex] + "," + event[longitudeIndex]
		}
		for index, value := range event {
			// skip if empty
			if event[index] != "" {
				// apply function if not empty
				kvPairs, err := knownFields[index].ParseFunction(knownFields[index].Key, value)
				if err != nil {
					return nil, err
				}
				// append all results
				for _, pair := range kvPairs {
					output[pair.Key] = pair.Value
				}
			}
		}
		return output, nil
	}
}

// Design decision made: Separated functions used to add Geo fields.
// These could be replaced by moving the functionality to the GetSubset functions - but it might be less intuitive.

// ToMap transforms a valid tsv string Snowplow event to a Go map.
func ToMap(event string) (map[string]interface{}, error) {
	record := strings.Split(event, "\t")
	return mapifyGoodEvent(record, enrichedEventFieldTypes, false)
}

// ToMapWithGeo adds the geo_location field, and transforms a valid tsv string Snowplow event to a Go map.
func ToMapWithGeo(event string) (map[string]interface{}, error) {
	record := strings.Split(event, "\t")
	return mapifyGoodEvent(record, enrichedEventFieldTypes, true)
}

// ToJson transforms a valid tsv string Snowplow event to a JSON object.
func ToJson(event string) ([]byte, error) {
	mapified, err := ToMap(event)
	if err != nil {
		return nil, err
	}

	jsonified, err := json.Marshal(mapified)
	if err != nil {
		return nil, errors.Wrap(err, "Error marshaling to JSON")
	}
	return jsonified, nil
}

// ToJsonWithGeo adds the geo_location field, and transforms a valid tsv string Snowplow event to a JSON object.
func ToJsonWithGeo(event string) ([]byte, error) {
	mapified, err := ToMapWithGeo(event)
	if err != nil {
		return nil, err
	}

	jsonified, err := json.Marshal(mapified)
	if err != nil {
		return nil, errors.Wrap(err, "Error marshaling to JSON")
	}
	return jsonified, nil
}

// Design question: If the field(s) provided to the below Get functions are (all) empty - should it return an error, a zero value, or nil, nil?
// Currently it returns nil, and an error for GetValue, and an empty map/json with no error for GetSubsetMap and GetSubsetJSON...
// I have a natrual aversion to zero value but not sure if it's well founded.

// Design decision made: unstruct_event, contexts and derived_contexts return the structure `{"unstruct_event_com_acme_event_1": {"field1": "value1"}}`

// GetValue returns the value for a provided atomic field, without processing the rest of the event.
// For unstruct_event, it returns a map of only the data for the unstruct event.
func GetValue(event string, field string) (interface{}, error) {

	// TODO: DRY HERE
	record := strings.Split(event, "\t")
	if len(record) != 131 { // leave hardcoded or not?
		return nil, errors.New("Cannot get value from event - wrong number of fields")
	} else {
		index, ok := indexMap[field]
		if !ok {
			return nil, errors.New(fmt.Sprintf("Key %s not a valid atomic field", field))
		}
		if record[index] == "" {
			return nil, errors.New(fmt.Sprintf("Field %s is empty", field))
		}
		kvPairs, err := enrichedEventFieldTypes[index].ParseFunction(enrichedEventFieldTypes[index].Key, record[index])
		if err != nil {
			return nil, err
		}
		if field == "contexts" || field == "derived_contexts" || field == "unstruct_event" {
			// TODO: DRY HERE TOO?
			output := make(map[string]interface{})
			for _, pair := range kvPairs {
				output[pair.Key] = pair.Value
			}
			return output, nil
		}
		return kvPairs[0].Value, nil
	}
}

// GetSubsetMap returns a map of a subset of the event, containing only the atomic fields provided, without processing the rest of the event.
// For custom events and contexts, only "unstruct_event", "contexts", or "derived_contexts" may be provided, which will produce the entire data object for that field.
// For contexts, the resultant map will contain all occurrences of all contexts within the provided field.
func GetSubsetMap(event string, fields []string) (map[string]interface{}, error) {
	// TODO: Same error handling issue as above - what should the behaviour be in case of no value?

	// TODO: DRY HERE
	record := strings.Split(event, "\t")
	if len(record) != 131 { // leave hardcoded or not?
		return nil, errors.New("Cannot get value from event - wrong number of fields")
	} else {
		// TODO: DRY HERE TOO
		output := make(map[string]interface{})
		for _, field := range fields {
			index, ok := indexMap[field]
			if !ok {
				return nil, errors.New(fmt.Sprintf("Key %s not a valid atomic field", field))
			}
			if record[index] != "" {
				kvPairs, err := enrichedEventFieldTypes[index].ParseFunction(enrichedEventFieldTypes[index].Key, record[index])
				if err != nil {
					return nil, err
				}

				for _, pair := range kvPairs {
					output[pair.Key] = pair.Value
				}
			}
		}
		return output, nil
	}
}

// GetSubsetJson returns a JSON object containing a subset of the event, containing only the atomic fields provided, without processing the rest of the event.
// For custom events and contexts, only "unstruct_event", "contexts", or "derived_contexts" may be provided, which will produce the entire data object for that field.
// For contexts, the resultant map will contain all occurrences of all contexts within the provided field.
func GetSubsetJson(event string, fields []string) ([]byte, error) {
	subsetMap, err := GetSubsetMap(event, fields)
	if err != nil {
		return nil, err
	}
	subsetJson, err := json.Marshal(subsetMap)
	if err != nil {
		return nil, err
	}
	return subsetJson, nil
}
