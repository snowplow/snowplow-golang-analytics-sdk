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

	"github.com/pkg/errors"

	jsoniter "github.com/json-iterator/go"
)

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

// TODO: This turns out to slow processing down a significant amount. Explore faster ways to achieve the same goal.
func extractSchemaOld(uri string) (SchemaParts, error) {
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

func extractSchema(uri string) (SchemaParts, error) {
	// fmt.Println(uri[5:]) // cut the protocol off.
	/* This makes things super slow.
	schema_pattern := regexp.MustCompile(SCHEMA_URI_REGEX)

	if !schema_pattern.MatchString(uri) {
		return SchemaParts{}, errors.New(fmt.Sprintf("Schema '%s' does not conform to regular expression '%s'", uri, SCHEMA_URI_REGEX))
	}
	*/

	formatErr := errors.New(fmt.Sprintf("Schema URI format error: %s", uri))

	splitProtocol := strings.SplitN(uri, ":", 2)
	if len(splitProtocol) != 2 || splitProtocol[0] == "" || splitProtocol[1] == "" {
		return SchemaParts{}, formatErr
	}

	splitParts := strings.Split(splitProtocol[1], "/")
	if len(splitParts) != 4 || splitParts[0] == "" || splitParts[1] == "" || splitParts[2] == "" || splitParts[3] == "" {
		return SchemaParts{}, errors.New(fmt.Sprintf("2222 Schema URI format error: %s", uri))
	}

	splitVersion := strings.SplitN(splitParts[len(splitParts)-1], "-", 2)
	if len(splitVersion) != 2 || splitVersion[0] == "" || splitVersion[1] == "" {
		return SchemaParts{}, errors.New(fmt.Sprintf("111 Schema URI format error: %s", uri))
	}

	// TODO: Consider:
	// The checks to make sure no part is empty allow us to pass the tests (covering invalid schema strings), but they add ~1200 ns to the benchmark. (200 -> 1400)
	// Correction - they do not actually...

	return SchemaParts{
		Vendor:   splitParts[0],
		Name:     splitParts[1],
		Format:   splitParts[2],
		Model:    splitVersion[0],
		Revision: splitVersion[1],
	}, nil
}

// iglu:org.w3/PerformanceTiming/jsonschema/1-0-0

// TODO: try out a new version of extractSchema where we pull out only the bits that matter to us.

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

	err := jsoniter.Unmarshal([]byte(contexts), &ctxts)
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

	err := jsoniter.Unmarshal([]byte(unstruct), &event)
	if err != nil {
		return nil, errors.Wrap(err, "Error unmarshaling unstruct event JSON")
	}

	key, err := fixSchema("unstruct_event", event.Data.Schema)
	if err != nil {
		return nil, errors.Wrap(err, "Error parsing unstruct event") // Too much nesting of error wrapping?
	}

	return []KeyVal{{key, event.Data.Data}}, nil
}
