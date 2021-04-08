package sdk // Terrible name... TODO: Come up with a better name

import (
	//	"encoding/base64"
	//	"encoding/csv"
	"encoding/json"
	// "errors" // TODO: Decide whether to use this or handle errors another way
	"fmt"
	"regexp"
	"strings"
	//	"time"
	"unicode" // For camel to snake case - consider alternative?
	// 	"github.com/hashicorp/go-multierror" // check out these packages for error handling
	"github.com/pkg/errors"
)

// This should be SelfDescribingData...
type SelfDescribingData struct {
	Schema string                 `json:"schema"` // Probably don't need the json tags
	Data   map[string]interface{} `json:"data"`

	// If we keep this as a string instead of a map, will it unmarshal into a string?
	// If so, I'm guessing it's preferable to do so and then marshal it to json as we assign to keys...
	// Data string `json:"data"`
	// OK so - error: json: cannot unmarshal object into Go struct field SelfDescribingData.data.data of type string
	// using map[string]string does work but still results in a map...
	// maybe leave it as []byte???
	// Data []byte - doesn't work.

}

type Contexts struct {
	Schema string               `json:"schema"`
	Data   []SelfDescribingData `json:"data"`
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

// Should be const??
var SCHEMA_URI_REGEX = `(?P<protocol>^iglu:)(?P<vendor>[a-zA-Z0-9-_.]+)/(?P<name>[a-zA-Z0-9-_]+)/(?P<format>[a-zA-Z0-9-_]+)/(?P<model>[1-9][0-9]*)(?P<revision>(?:-(?:0|[1-9][0-9]*)){2}$)`
// Take regex capture group names out, as not used?
// https://golang.org/pkg/regexp/#example_Regexp_SubexpNames

func extractSchema(uri string) (SchemaParts, error) {
	schema_pattern := regexp.MustCompile(SCHEMA_URI_REGEX)

	match := schema_pattern.FindStringSubmatch(uri)
	if match != nil {
		// fmt.Println(match)
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
// Should this avoid double underscores for 'mix_Of_Camel_And_Snake_Case'?
func insertUnderscores(s string) string {
	var res = make([]rune, 0, len(s))
	for i, r := range s {
		if unicode.IsUpper(r) && i > 0 {
			res = append(res, '_', r)
		} else {
			res = append(res, r)
		}
		// j := i  .... add j != _ to condition above?
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

func shredContexts(contexts string) ([]KeyVals, error) {

	ctxts := Contexts{}

	err := json.Unmarshal([]byte(contexts), &ctxts)
	if err != nil {
		return nil, errors.Wrap(err, "Error unmarshaling context JSON")
	}

	var distinctContexts = make(map[string][]interface{})
	for _, entry := range ctxts.Data {
		key, err := fixSchema("contexts", entry.Schema) // is key a crap var name here?
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

	out := make([]KeyVals, 0, len(distinctContexts))
	outPointer := &out
	for key, val := range distinctContexts {
		*outPointer = append(*outPointer, KeyVals{key, val})
	}
	return out, nil
}

func shredUnstruct(unstruct string) ([]KeyVals, error) {

	event := UnstructEvent{}

	err := json.Unmarshal([]byte(unstruct), &event)
	if err != nil {
		return nil, errors.Wrap(err, "Error unmarshaling unstruct event JSON")
	}

	key, err := fixSchema("unstruct_event", event.Data.Schema) 
	if err != nil {
		return nil, errors.Wrap(err, "Error parsing unstruct event") // Too much nesting of error wrapping?
	}

	return []KeyVals{KeyVals{key, event.Data.Data}}, nil
}
