package sdk

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// TODO: Add equality checks for specific error messages
// TODO: Add benchmarking

func TestExtractSchema(t *testing.T) {
	assert := assert.New(t)

	schemaParts, err := extractSchema("iglu:com.acme.data/some_event/jsonschema/15-34-1")
	schemaParts2, err2 := extractSchema("com.acme.notvalid/invalidschemapath/jsonschema/1.0.0")

	assert.Nil(err)
	assert.Equal("iglu:", schemaParts.Protocol)
	assert.Equal("com.acme.data", schemaParts.Vendor)

	assert.Equal("some_event", schemaParts.Name)
	assert.Equal("jsonschema", schemaParts.Format)

	assert.Equal("15", schemaParts.Model)
	assert.Equal("-34-1", schemaParts.Revision)

	assert.NotNil(err2)
	assert.Zero(schemaParts2.Protocol)
	assert.Zero(schemaParts2.Vendor)
	assert.Zero(schemaParts2.Name)
	assert.Zero(schemaParts2.Format)
	assert.Zero(schemaParts2.Model)
	assert.Zero(schemaParts2.Revision)
}

func BenchmarkExtractSchema(b *testing.B) {
	for i := 0; i < b.N; i++ {
		extractSchema("iglu:com.acme.data/some_event/jsonschema/15-34-1")
	}
}

func TestInsertUnderscores(t *testing.T) {
	assert := assert.New(t)

	underscoredCamelCase := insertUnderscores("ThisStringIsCamelCase")
	underscoredMixture := insertUnderscores("this_StringIsAMixture")

	assert.Equal("This_String_Is_Camel_Case", underscoredCamelCase)
	assert.Equal("this__String_Is_A_Mixture", underscoredMixture) // should our function avoid double-underscore in this case???
}

func BenchmarkInsertUnderscores(b *testing.B) {
	for i := 0; i < b.N; i++ {
		insertUnderscores("ThisStringIsCamelCase")
	}
}

func TestFixSchema(t *testing.T) {
	assert := assert.New(t)

	fixedSchema, err := fixSchema("unstruct", "iglu:com.acme.data/some_event/jsonschema/15-34-1")
	brokenSchema, err2 := fixSchema("unstruct", "iglu:com.broken.path//jsonschema/1-0-0")

	assert.Nil(err)
	assert.Equal("unstruct_com_acme_data_some_event_15", fixedSchema)
	assert.NotNil(err2)
	assert.Zero(brokenSchema)
}

func BenchmarkFixSchema(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fixSchema("unstruct", "iglu:com.acme.data/some_event/jsonschema/15-34-1")
	}
}

func TestShredContexts(t *testing.T) {
	assert := assert.New(t)

	ctxt := `{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.acme/test_context/jsonschema/1-0-0","data":{"field1": 1}}, {"schema":"iglu:com.acme/test_context/jsonschema/1-0-0","data":{"field1": 2}}]}`
	ctxt2 := `{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"fail","data":{"field1": 1}}]}`

	map1 := map[string]interface{}{"field1": 1.0} // using decimals as the interface value is interpreted as float64
	map2 := map[string]interface{}{"field1": 2.0}
	var expected = []KeyVals{KeyVals{"contexts_com_acme_test_context_1", []interface{}{map1, map2}}}

	shreddedContexts, err := shredContexts(ctxt)

	failedShred, err2 := shredContexts(ctxt2)

	assert.Nil(err)
	assert.Equal(expected, shreddedContexts)

	assert.NotNil(err2)
	assert.Nil(failedShred)
}

func BenchmarkShredContexts(b *testing.B) {
	// move to global
	ctxt := `{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.acme/test_context/jsonschema/1-0-0","data":{"field1": 1}}, {"schema":"iglu:com.acme/test_context/jsonschema/1-0-0","data":{"field1": 2}}]}`
	for i := 0; i < b.N; i++ {
		shredContexts(ctxt)
	}
}

func TestShredUnstruct(t *testing.T) {
	assert := assert.New(t)
	// move to global
	unstruct := `{"data":{"data":{"key":"value"},"schema":"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1"},"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0"}`
	unstruct2 := `{"data":{"data":{"key":"value"},"schema":"fail"},"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0"}`

	map1 := map[string]interface{}{"key": "value"}
	expected := []KeyVals{KeyVals{"unstruct_event_com_snowplowanalytics_snowplow_link_click_1", map1}}

	shreddedUnstruct, err := shredUnstruct(unstruct)
	failedShred, err2 := shredUnstruct(unstruct2)

	assert.Nil(err)
	assert.Equal(expected, shreddedUnstruct)

	assert.NotNil(err2)
	assert.Nil(failedShred)
}

func BenchmarkShredUnstruct(b *testing.B) {
	unstruct := `{"data":{"data":{"key":"value"},"schema":"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1"},"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0"}`
	for i := 0; i < b.N; i++ {
		shredUnstruct(unstruct)
	}
}
