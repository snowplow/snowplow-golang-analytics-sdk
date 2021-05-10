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
	"regexp"
	"strings"
	"unicode" // For camel to snake case - consider alternative?

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

var ConfigCompatibleWithStandardLibrary = jsoniter.Config{EscapeHTML: false}
var json = jsoniter.ConfigCompatibleWithStandardLibrary

type SelfDescribingData struct {
	Schema string
	Data   map[string]interface{} // TODO: See if leaving data as a string or byte array would work, and would be faster.
}

type Contexts struct {
	Schema string
	Data   []SelfDescribingData
}

type UnstructEvent struct {
	Schema string
	Data   SelfDescribingData
}

type SchemaParts struct {
	Protocol string
	Vendor   string
	Name     string
	Format   string
	Model    string
	Revision string
}

const SCHEMA_URI_REGEX string = `(?P<protocol>^iglu:)(?P<vendor>[a-zA-Z0-9-_.]+)/(?P<name>[a-zA-Z0-9-_]+)/(?P<format>[a-zA-Z0-9-_]+)/(?P<model>[1-9][0-9]*)(?P<revision>(?:-(?:0|[1-9][0-9]*)){2}$)`

// Take regex capture group names out, as not used?
// https://golang.org/pkg/regexp/#example_Regexp_SubexpNames

func extractSchema(uri string) (SchemaParts, error) {
	schema_pattern := regexp.MustCompile(SCHEMA_URI_REGEX)

	match := schema_pattern.FindStringSubmatch(uri)
	if match != nil {
		return SchemaParts{
			Protocol: match[1],
			Vendor:   match[2],
			Name:     match[3],
			Format:   match[4],
			Model:    match[5],
			Revision: match[6],
		}, nil
	} else {
		return SchemaParts{}, errors.New(fmt.Sprintf("Schema '%s' does not conform to regular expression '%s'", uri, SCHEMA_URI_REGEX))
	}

}

// Based on https://gist.github.com/stoewer/fbe273b711e6a06315d19552dd4d33e6#gistcomment-3673823
func insertUnderscores(s string) string {
	var res = make([]rune, 0, len(s))
	var prev rune
	for i, r := range s {
		if unicode.IsUpper(r) && i > 0 && prev != '_' {
			res = append(res, '_', r)
		} else {
			res = append(res, r)
		}
		prev = r
	}
	return string(res)
}

func fixSchema(prefix string, schemaUri string) (string, error) {
	parts, err := extractSchema(schemaUri)
	if err != nil {
		return "", errors.Wrap(err, "Error parsing schema path")
	}
	vendor := strings.Replace(parts.Vendor, ".", "_", -1)
	name := insertUnderscores(parts.Name)

	return strings.ToLower(strings.Join([]string{prefix, vendor, name, parts.Model}, "_")), nil
}

func shredContexts(contexts string) ([]KeyVal, error) {
	ctxts := Contexts{}

	err := json.Unmarshal([]byte(contexts), &ctxts)
	if err != nil {
		return nil, errors.Wrap(err, "Error unmarshaling context JSON")
	}

	var distinctContexts = make(map[string][]interface{})
	for _, entry := range ctxts.Data {
		key, err := fixSchema("contexts", entry.Schema) // is key a bad var name here?
		if err != nil {
			return nil, errors.Wrap(err, "Error parsing contexts") // Too much nesting of error wrapping?
		}

		data := entry.Data

		if _, present := distinctContexts[key]; present {
			distinctContexts[key] = append(distinctContexts[key], data)
		} else {
			distinctContexts[key] = make([]interface{}, 1)
			distinctContexts[key][0] = data
		}
	}

	out := make([]KeyVal, 0, len(distinctContexts))
	outPointer := &out
	for key, val := range distinctContexts {
		*outPointer = append(*outPointer, KeyVal{key, val})
	}
	return out, nil
}

func shredUnstruct(unstruct string) ([]KeyVal, error) {

	event := UnstructEvent{}

	err := json.Unmarshal([]byte(unstruct), &event)
	if err != nil {
		return nil, errors.Wrap(err, "Error unmarshaling unstruct event JSON")
	}

	key, err := fixSchema("unstruct_event", event.Data.Schema)
	if err != nil {
		return nil, errors.Wrap(err, "Error parsing unstruct event") // Too much nesting of error wrapping?
	}

	return []KeyVal{{key, event.Data.Data}}, nil
}
