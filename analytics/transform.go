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
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

const (
	eventLength   int    = 131
	EmptyFieldErr string = `Field is empty`
)

var json = jsoniter.Config{}.Froze()

type KeyVal struct {
	Key   string
	Value interface{}
}

type ValueParser func(string, string) ([]KeyVal, error)

type KeyFunctionPair struct {
	Key           string
	ParseFunction ValueParser
}

type ParsedEvent []string

func parseTime(key string, value string) ([]KeyVal, error) {
	if value == "" {
		return nil, errors.Wrap(errors.New("Null string found"), fmt.Sprintf("Error parsing key %s", key))
	}
	timeLayout := "2006-01-02 15:04:05.999"
	out, err := time.Parse(timeLayout, value)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Error parsing field '%s', with value '%s' to timestamp", key, value))
	}
	return []KeyVal{{key, out}}, err
}

func parseString(key string, value string) ([]KeyVal, error) {
	if value == "" {
		return nil, errors.Wrap(errors.New("Null string found"), fmt.Sprintf("Error parsing key %s", key))
	}
	return []KeyVal{{key, value}}, nil
}

func parseInt(key string, value string) ([]KeyVal, error) {
	if value == "" {
		return nil, errors.Wrap(errors.New("Null string found"), fmt.Sprintf("Error parsing key %s", key))
	}
	intValue, err := strconv.Atoi(value)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Error parsing key '%s' to integer", key))
	}
	return []KeyVal{{key, intValue}}, err
}

func parseBool(key string, value string) ([]KeyVal, error) {
	if value == "" {
		return nil, errors.Wrap(errors.New("Null string found"), fmt.Sprintf("Error parsing key %s", key))
	}
	boolValue, err := strconv.ParseBool(value)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Error parsing key '%s' to boolean", key))
	}
	return []KeyVal{{key, boolValue}}, err
}

func parseDouble(key string, value string) ([]KeyVal, error) {
	if value == "" {
		return nil, errors.Wrap(errors.New("Null string found"), fmt.Sprintf("Error parsing key %s", key))
	}
	doubleValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Error parsing key '%s' to double", key))
	}
	return []KeyVal{{key, doubleValue}}, err
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

// ParseEvent takes a Snowplow Enriched event tsv string as input, and returns a 'ParsedEvent' typed slice of strings.
// Methods may then be called on the resulting ParsedEvent type to transform the event, or a subset of the event to Map or Json.
func ParseEvent(event string) (ParsedEvent, error) {
	record := strings.Split(event, "\t")
	if len(record) != eventLength {
		return nil, errors.New(fmt.Sprintf("Cannot parse tsv event - wrong number of fields provided: %v", len(record)))
	}
	return record, nil
}

func (event ParsedEvent) mapifyGoodEvent(knownFields [131]KeyFunctionPair, addGeolocationData bool) (map[string]interface{}, error) {
	if len(event) != eventLength {
		return nil, errors.New(fmt.Sprintf("Cannot transform event - wrong number of fields provided: %v", len(event)))
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

// ToMap transforms a valid Snowplow ParsedEvent to a Go map.
func (event ParsedEvent) ToMap() (map[string]interface{}, error) {
	return event.mapifyGoodEvent(enrichedEventFieldTypes, false)
}

// ToMapWithGeo adds the geo_location field, and transforms a valid Snowplow ParsedEvent to a Go map.
func (event ParsedEvent) ToMapWithGeo() (map[string]interface{}, error) {
	return event.mapifyGoodEvent(enrichedEventFieldTypes, true)
}

// ToJson transforms a valid Snowplow ParsedEvent to a JSON object.
func (event ParsedEvent) ToJson() ([]byte, error) {

	mapified, err := event.ToMap()
	if err != nil {
		return nil, err
	}

	jsonified, err := json.Marshal(mapified)
	if err != nil {
		return nil, errors.Wrap(err, "Error marshaling to JSON")
	}
	return jsonified, nil
}

// ToJsonWithGeo adds the geo_location field, and transforms a valid Snowplow ParsedEvent to a JSON object.
func (event ParsedEvent) ToJsonWithGeo() ([]byte, error) {
	mapified, err := event.ToMapWithGeo()
	if err != nil {
		return nil, err
	}

	jsonified, err := json.Marshal(mapified)
	if err != nil {
		return nil, errors.Wrap(err, "Error marshaling to JSON")
	}
	return jsonified, nil
}

// getParsedValue gets a field's value from an event after parsing it with its specific ParseFunction
func (event ParsedEvent) getParsedValue(field string) ([]KeyVal, error) {
	if len(event) != eventLength {
		return nil, errors.New(fmt.Sprintf("Cannot get value - wrong number of fields provided: %v", len(event)))
	}
	index, ok := indexMap[field]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Key %s not a valid atomic field", field))
	}
	if event[index] == "" {
		return nil, errors.New(EmptyFieldErr)
	}
	kvPairs, err := enrichedEventFieldTypes[index].ParseFunction(enrichedEventFieldTypes[index].Key, event[index])
	if err != nil {
		return nil, err
	}

	return kvPairs, nil
}

// GetValue returns the value for a provided atomic field, without processing the rest of the event.
// For unstruct_event, it returns a map of only the data for the unstruct event.
func (event ParsedEvent) GetValue(field string) (interface{}, error) {
	kvPairs, err := event.getParsedValue(field)
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

// GetUnstructEventValue returns the value for a provided atomic field inside an event's unstruct_event field
func (event ParsedEvent) GetUnstructEventValue(path ...interface{}) (interface{}, error) {
	fullPath := append([]interface{}{`data`, `data`}, path...)

	el := json.Get([]byte(event[indexMap["unstruct_event"]]), fullPath...)
	return el.GetInterface(), el.LastError()
}

// GetContextValue returns the value for a provided atomic field inside an event's contexts or derived_contexts
func (event ParsedEvent) GetContextValue(contextName string, path ...interface{}) (interface{}, error) {
	contextNames := []string{`contexts`, `derived_contexts`}
	var contexts []interface{}
	for _, c := range contextNames {
		kvPairs, err := event.getParsedValue(c)
		if err != nil && err.Error() != EmptyFieldErr {
			return nil, err
		}
		// extract the key/value pairs of the event path into a map
		eventMap := make(map[string]interface{})
		for _, pair := range kvPairs {
			eventMap[pair.Key] = pair.Value
		}
		contexts = append(contexts, eventMap)
	}

	var output []interface{}
	b := make([]interface{}, len(path))
	for idx := range path {
		b[idx] = path[idx]
	}

	// iterate through all contextNames and extract the requested path to the output slice
	for _, ctx := range contexts {
		for key, contextSlice := range ctx.(map[string]interface{}) {
			if key == contextName {
				for _, ctxValues := range contextSlice.([]interface{}) {
					ctxValuesMap := ctxValues.(map[string]interface{})
					// output whole context if path is not defined
					if len(path) == 0 {
						output = append(output, ctxValuesMap)
						continue
					}
					j, err := json.Marshal(ctxValuesMap)
					if err != nil {
						return nil, err
					}
					el := json.Get(j, b...)
					if el.LastError() == nil {
						output = append(output, el.GetInterface())
					}
				}
			}
		}
	}
	return output, nil
}

// GetSubsetMap returns a map of a subset of the event, containing only the atomic fields provided, without processing the rest of the event.
// For custom events and contexts, only "unstruct_event", "contexts", or "derived_contexts" may be provided, which will produce the entire data object for that field.
// For contexts, the resultant map will contain all occurrences of all contexts within the provided field.
func (event ParsedEvent) GetSubsetMap(fields ...string) (map[string]interface{}, error) {

	if len(event) != eventLength {
		return nil, errors.New(fmt.Sprintf("Cannot get values - wrong number of fields provided: %v", len(event)))
	}
	output := make(map[string]interface{})
	for _, field := range fields {
		index, ok := indexMap[field]
		if !ok {
			return nil, errors.New(fmt.Sprintf("Key %s not a valid atomic field", field))
		}
		if event[index] != "" {
			kvPairs, err := enrichedEventFieldTypes[index].ParseFunction(enrichedEventFieldTypes[index].Key, event[index])
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

// GetSubsetJson returns a JSON object containing a subset of the event, containing only the atomic fields provided, without processing the rest of the event.
// For custom events and contexts, only "unstruct_event", "contexts", or "derived_contexts" may be provided, which will produce the entire data object for that field.
// For contexts, the resultant map will contain all occurrences of all contexts within the provided field.
func (event ParsedEvent) GetSubsetJson(fields ...string) ([]byte, error) {

	if len(event) != eventLength {
		return nil, errors.New(fmt.Sprintf("Cannot get values - wrong number of fields provided: %v", len(event)))
	}
	subsetMap, err := event.GetSubsetMap(fields...)
	if err != nil {
		return nil, err
	}
	subsetJson, err := json.Marshal(subsetMap)
	if err != nil {
		return nil, err
	}
	return subsetJson, nil
}
