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
)

// schema_pattern is compiled once at package initialization for performance
var schema_pattern = regexp.MustCompile(SCHEMA_URI_REGEX)

type SelfDescribingData struct {
	Schema string
	Data   map[string]any // TODO: See if leaving data as a string or byte array would work, and would be faster.
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
		return SchemaParts{}, fmt.Errorf("schema '%s' does not conform to regular expression '%s'", uri, SCHEMA_URI_REGEX)
	}

}

func insertUnderscores(s string) string {
	if len(s) == 0 {
		return s
	}

	var b strings.Builder
	b.Grow(len(s) + len(s)/4) // pre-allocate ~25% extra for underscores

	for i, r := range s {
		if unicode.IsUpper(r) && i > 0 {
			prev := rune(s[i-1])
			if prev != '_' {
				b.WriteRune('_')
			}
		}
		b.WriteRune(r)
	}
	return b.String()
}

// fixSchema transforms a schema URI into a normalized name with vendor prefix.
// Uses a thread-safe cache to avoid repeated regex parsing and string processing.
// Cache key format: "prefix:schemaUri" for unique lookups per prefix-schema combination.
// Default cache size: 1000 entries, configurable via SetSchemaCacheConfig().
func fixSchema(prefix string, schemaUri string) (string, error) {
	cacheKey := prefix + ":" + schemaUri
	if cached, ok := schemaCache.get(cacheKey); ok {
		return cached, nil
	}

	parts, err := extractSchema(schemaUri)
	if err != nil {
		return "", fmt.Errorf("error parsing schema path: %w", err)
	}
	vendor := strings.ReplaceAll(parts.Vendor, ".", "_")
	name := insertUnderscores(parts.Name)

	result := strings.ToLower(strings.Join([]string{prefix, vendor, name, parts.Model}, "_"))

	schemaCache.put(cacheKey, result)

	return result, nil
}

// shredContexts extracts self-describing contexts from the contexts array and groups
// them by schema. Returns key-value pairs where keys are normalized schema names
// and values are arrays of context data.
//
// Performance optimizations:
// - Pre-allocated maps (capacity 8) and slices (capacity 4) reduce allocations
// - Uses cached schema lookups via fixSchema for performance
func shredContexts(contexts string) ([]KeyVal, error) {
	ctxts := Contexts{}

	err := jsoniter.Unmarshal([]byte(contexts), &ctxts)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling context JSON: %w", err)
	}

	var distinctContexts = make(map[string][]any, 8)
	for _, entry := range ctxts.Data {
		key, err := fixSchema("contexts", entry.Schema) // is key a bad var name here?
		if err != nil {
			return nil, fmt.Errorf("error parsing contexts: %w", err)
		}

		data := entry.Data

		if _, present := distinctContexts[key]; present {
			distinctContexts[key] = append(distinctContexts[key], data)
		} else {
			distinctContexts[key] = make([]any, 0, 4)
			distinctContexts[key] = append(distinctContexts[key], data)
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
		return nil, fmt.Errorf("error unmarshaling unstruct event JSON: %w", err)
	}

	key, err := fixSchema("unstruct_event", event.Data.Schema)
	if err != nil {
		return nil, fmt.Errorf("error parsing unstruct event: %w", err)
	}

	return []KeyVal{{key, event.Data.Data}}, nil
}
