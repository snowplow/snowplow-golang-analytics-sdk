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

var enrichedEventFieldTypes = [131]KeyFunctionPair{KeyFunctionPair{"app_id", parseString},
	KeyFunctionPair{"platform", parseString},
	KeyFunctionPair{"etl_tstamp", parseTime},
	KeyFunctionPair{"collector_tstamp", parseTime},
	KeyFunctionPair{"dvce_created_tstamp", parseTime},
	KeyFunctionPair{"event", parseString},
	KeyFunctionPair{"event_id", parseString},
	KeyFunctionPair{"txn_id", parseInt},
	KeyFunctionPair{"name_tracker", parseString},
	KeyFunctionPair{"v_tracker", parseString},
	KeyFunctionPair{"v_collector", parseString},
	KeyFunctionPair{"v_etl", parseString},
	KeyFunctionPair{"user_id", parseString},
	KeyFunctionPair{"user_ipaddress", parseString},
	KeyFunctionPair{"user_fingerprint", parseString},
	KeyFunctionPair{"domain_userid", parseString},
	KeyFunctionPair{"domain_sessionidx", parseInt},
	KeyFunctionPair{"network_userid", parseString},
	KeyFunctionPair{"geo_country", parseString},
	KeyFunctionPair{"geo_region", parseString},
	KeyFunctionPair{"geo_city", parseString},
	KeyFunctionPair{"geo_zipcode", parseString},
	KeyFunctionPair{"geo_latitude", parseDouble},
	KeyFunctionPair{"geo_longitude", parseDouble},
	KeyFunctionPair{"geo_region_name", parseString},
	KeyFunctionPair{"ip_isp", parseString},
	KeyFunctionPair{"ip_organization", parseString},
	KeyFunctionPair{"ip_domain", parseString},
	KeyFunctionPair{"ip_netspeed", parseString},
	KeyFunctionPair{"page_url", parseString},
	KeyFunctionPair{"page_title", parseString},
	KeyFunctionPair{"page_referrer", parseString},
	KeyFunctionPair{"page_urlscheme", parseString},
	KeyFunctionPair{"page_urlhost", parseString},
	KeyFunctionPair{"page_urlport", parseInt},
	KeyFunctionPair{"page_urlpath", parseString},
	KeyFunctionPair{"page_urlquery", parseString},
	KeyFunctionPair{"page_urlfragment", parseString},
	KeyFunctionPair{"refr_urlscheme", parseString},
	KeyFunctionPair{"refr_urlhost", parseString},
	KeyFunctionPair{"refr_urlport", parseInt},
	KeyFunctionPair{"refr_urlpath", parseString},
	KeyFunctionPair{"refr_urlquery", parseString},
	KeyFunctionPair{"refr_urlfragment", parseString},
	KeyFunctionPair{"refr_medium", parseString},
	KeyFunctionPair{"refr_source", parseString},
	KeyFunctionPair{"refr_term", parseString},
	KeyFunctionPair{"mkt_medium", parseString},
	KeyFunctionPair{"mkt_source", parseString},
	KeyFunctionPair{"mkt_term", parseString},
	KeyFunctionPair{"mkt_content", parseString},
	KeyFunctionPair{"mkt_campaign", parseString},
	KeyFunctionPair{"contexts", parseContexts},
	KeyFunctionPair{"se_category", parseString},
	KeyFunctionPair{"se_action", parseString},
	KeyFunctionPair{"se_label", parseString},
	KeyFunctionPair{"se_property", parseString},
	KeyFunctionPair{"se_value", parseString},
	KeyFunctionPair{"unstruct_event", parseUnstruct},
	KeyFunctionPair{"tr_orderid", parseString},
	KeyFunctionPair{"tr_affiliation", parseString},
	KeyFunctionPair{"tr_total", parseDouble},
	KeyFunctionPair{"tr_tax", parseDouble},
	KeyFunctionPair{"tr_shipping", parseDouble},
	KeyFunctionPair{"tr_city", parseString},
	KeyFunctionPair{"tr_state", parseString},
	KeyFunctionPair{"tr_country", parseString},
	KeyFunctionPair{"ti_orderid", parseString},
	KeyFunctionPair{"ti_sku", parseString},
	KeyFunctionPair{"ti_name", parseString},
	KeyFunctionPair{"ti_category", parseString},
	KeyFunctionPair{"ti_price", parseDouble},
	KeyFunctionPair{"ti_quantity", parseInt},
	KeyFunctionPair{"pp_xoffset_min", parseInt},
	KeyFunctionPair{"pp_xoffset_max", parseInt},
	KeyFunctionPair{"pp_yoffset_min", parseInt},
	KeyFunctionPair{"pp_yoffset_max", parseInt},
	KeyFunctionPair{"useragent", parseString},
	KeyFunctionPair{"br_name", parseString},
	KeyFunctionPair{"br_family", parseString},
	KeyFunctionPair{"br_version", parseString},
	KeyFunctionPair{"br_type", parseString},
	KeyFunctionPair{"br_renderengine", parseString},
	KeyFunctionPair{"br_lang", parseString},
	KeyFunctionPair{"br_features_pdf", parseBool},
	KeyFunctionPair{"br_features_flash", parseBool},
	KeyFunctionPair{"br_features_java", parseBool},
	KeyFunctionPair{"br_features_director", parseBool},
	KeyFunctionPair{"br_features_quicktime", parseBool},
	KeyFunctionPair{"br_features_realplayer", parseBool},
	KeyFunctionPair{"br_features_windowsmedia", parseBool},
	KeyFunctionPair{"br_features_gears", parseBool},
	KeyFunctionPair{"br_features_silverlight", parseBool},
	KeyFunctionPair{"br_cookies", parseBool},
	KeyFunctionPair{"br_colordepth", parseString},
	KeyFunctionPair{"br_viewwidth", parseInt},
	KeyFunctionPair{"br_viewheight", parseInt},
	KeyFunctionPair{"os_name", parseString},
	KeyFunctionPair{"os_family", parseString},
	KeyFunctionPair{"os_manufacturer", parseString},
	KeyFunctionPair{"os_timezone", parseString},
	KeyFunctionPair{"dvce_type", parseString},
	KeyFunctionPair{"dvce_ismobile", parseBool},
	KeyFunctionPair{"dvce_screenwidth", parseInt},
	KeyFunctionPair{"dvce_screenheight", parseInt},
	KeyFunctionPair{"doc_charset", parseString},
	KeyFunctionPair{"doc_width", parseInt},
	KeyFunctionPair{"doc_height", parseInt},
	KeyFunctionPair{"tr_currency", parseString},
	KeyFunctionPair{"tr_total_base", parseDouble},
	KeyFunctionPair{"tr_tax_base", parseDouble},
	KeyFunctionPair{"tr_shipping_base", parseDouble},
	KeyFunctionPair{"ti_currency", parseString},
	KeyFunctionPair{"ti_price_base", parseDouble},
	KeyFunctionPair{"base_currency", parseString},
	KeyFunctionPair{"geo_timezone", parseString},
	KeyFunctionPair{"mkt_clickid", parseString},
	KeyFunctionPair{"mkt_network", parseString},
	KeyFunctionPair{"etl_tags", parseString},
	KeyFunctionPair{"dvce_sent_tstamp", parseTime},
	KeyFunctionPair{"refr_domain_userid", parseString},
	KeyFunctionPair{"refr_device_tstamp", parseTime},
	KeyFunctionPair{"derived_contexts", parseContexts},
	KeyFunctionPair{"domain_sessionid", parseString},
	KeyFunctionPair{"derived_tstamp", parseTime},
	KeyFunctionPair{"event_vendor", parseString},
	KeyFunctionPair{"event_name", parseString},
	KeyFunctionPair{"event_format", parseString},
	KeyFunctionPair{"event_version", parseString},
	KeyFunctionPair{"event_fingerprint", parseString},
	KeyFunctionPair{"true_tstamp", parseTime}}

var latitudeIndex = 22
var longitudeIndex = 23

func parseNullableTime(timeString string) (*time.Time, error) { // Probably no need for a pointer here since we're manually parsing the whole thing
	timeLayout := "2006-01-02 15:04:05.999"
	res, err := time.Parse(timeLayout, timeString)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Error parsing timestamp value '%s'", timeString))
	}
	if time.Time.IsZero(res) {
		return nil, errors.New(fmt.Sprintf("Timestamp string '%s' resulted in zero-value timestamp", timeString))
	} else {
		return &res, nil
	}
}

func parseTime(key string, value string) ([]KeyVal, error) {
	out, err := parseNullableTime(value)
	if err != nil {
		return nil, errors.Wrap(err, key)
	}
	return []KeyVal{KeyVal{key, out}}, err
}

func parseString(key string, value string) ([]KeyVal, error) { // throw an error if it's a zero string?
	if value == "" {
		return nil, errors.Wrap(errors.New("Zero value found for string"), key)
	}
	return []KeyVal{KeyVal{key, value}}, nil
}

func parseInt(key string, value string) ([]KeyVal, error) {
	intValue, err := strconv.Atoi(value)
	if err != nil {
		return nil, errors.Wrap(err, key) // maybe an error message as well as the key? "Cannot parse field '%s'"? - in fact maybe there should be a specific error class for it?
	}
	return []KeyVal{KeyVal{key, intValue}}, err
}

func parseBool(key string, value string) ([]KeyVal, error) {
	boolValue, err := strconv.ParseBool(value)
	if err != nil {
		return nil, errors.Wrap(err, key)
	}
	return []KeyVal{KeyVal{key, boolValue}}, err
}

func parseDouble(key string, value string) ([]KeyVal, error) {
	doubleValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, errors.Wrap(err, key)
	}
	return []KeyVal{KeyVal{key, doubleValue}}, err
}

func parseContexts(key string, value string) ([]KeyVal, error) {
	return shredContexts(value)
}

func parseUnstruct(key string, value string) ([]KeyVal, error) {
	return shredUnstruct(value)
}

// event is slice because csv package outputs a slice.

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

/* Since Golang tries its hardest to design against optional parameters, electing to implement the main Transform function
to mirror the most common usage of the function - with addGeolocationData set to true (ie the default in the other SDKs).

Elected to make a function to specifically transform to JSON, with a view to one to transform to Map also
This means that having addGeolocationData might lead to proliferation of functions... */

// TransformToJson transforms a valid tsv string Snowplow event to a JSON object.
// It also adds the geo_location field.
func TransformToJson(event string) ([]byte, error) {
	mapified, err := TransformToMap(event)
	if err != nil {
		return nil, err
	}

	jsonified, err := json.Marshal(mapified)
	if err != nil {
		return nil, errors.Wrap(err, "Error marshaling to JSON")
	}
	return jsonified, nil
}

// TransformToMap transforms a valid tsv string Snowplow event to a Go map. It also adds the geo_location field.
func TransformToMap(event string) (map[string]interface{}, error) {
	record := strings.Split(event, "\t")
	return mapifyGoodEvent(record, enrichedEventFieldTypes, true)
}

// Rename these? eg. just ToJson / ToMap?

var indexMap = map[string]int16{"app_id": 0,
	"platform": 1,
	"etl_tstamp": 2,
	"collector_tstamp": 3,
	"dvce_created_tstamp": 4,
	"event": 5,
	"event_id": 6,
	"txn_id": 7,
	"name_tracker": 8,
	"v_tracker": 9,
	"v_collector": 10,
	"v_etl": 11,
	"user_id": 12,
	"user_ipaddress": 13,
	"user_fingerprint": 14,
	"domain_userid": 15,
	"domain_sessionidx": 16,
	"network_userid": 17,
	"geo_country": 18,
	"geo_region": 19,
	"geo_city": 20,
	"geo_zipcode": 21,
	"geo_latitude": 22,
	"geo_longitude": 23,
	"geo_region_name": 24,
	"ip_isp": 25,
	"ip_organization": 26,
	"ip_domain": 27,
	"ip_netspeed": 28,
	"page_url": 29,
	"page_title": 30,
	"page_referrer": 31,
	"page_urlscheme": 32,
	"page_urlhost": 33,
	"page_urlport": 34,
	"page_urlpath": 35,
	"page_urlquery": 36,
	"page_urlfragment": 37,
	"refr_urlscheme": 38,
	"refr_urlhost": 39,
	"refr_urlport": 40,
	"refr_urlpath": 41,
	"refr_urlquery": 42,
	"refr_urlfragment": 43,
	"refr_medium": 44,
	"refr_source": 45,
	"refr_term": 46,
	"mkt_medium": 47,
	"mkt_source": 48,
	"mkt_term": 49,
	"mkt_content": 50,
	"mkt_campaign": 51,
	"contexts": 52,
	"se_category": 53,
	"se_action": 54,
	"se_label": 55,
	"se_property": 56,
	"se_value": 57,
	"unstruct_event": 58,
	"tr_orderid": 59,
	"tr_affiliation": 60,
	"tr_total": 61,
	"tr_tax": 62,
	"tr_shipping": 63,
	"tr_city": 64,
	"tr_state": 65,
	"tr_country": 66,
	"ti_orderid": 67,
	"ti_sku": 68,
	"ti_name": 69,
	"ti_category": 70,
	"ti_price": 71,
	"ti_quantity": 72,
	"pp_xoffset_min": 73,
	"pp_xoffset_max": 74,
	"pp_yoffset_min": 75,
	"pp_yoffset_max": 76,
	"useragent": 77,
	"br_name": 78,
	"br_family": 79,
	"br_version": 80,
	"br_type": 81,
	"br_renderengine": 82,
	"br_lang": 83,
	"br_features_pdf": 84,
	"br_features_flash": 85,
	"br_features_java": 86,
	"br_features_director": 87,
	"br_features_quicktime": 88,
	"br_features_realplayer": 89,
	"br_features_windowsmedia": 90,
	"br_features_gears": 91,
	"br_features_silverlight": 92,
	"br_cookies": 93,
	"br_colordepth": 94,
	"br_viewwidth": 95,
	"br_viewheight": 96,
	"os_name": 97,
	"os_family": 98,
	"os_manufacturer": 99,
	"os_timezone": 100,
	"dvce_type": 101,
	"dvce_ismobile": 102,
	"dvce_screenwidth": 103,
	"dvce_screenheight": 104,
	"doc_charset": 105,
	"doc_width": 106,
	"doc_height": 107,
	"tr_currency": 108,
	"tr_total_base": 109,
	"tr_tax_base": 110,
	"tr_shipping_base": 111,
	"ti_currency": 112,
	"ti_price_base": 113,
	"base_currency": 114,
	"geo_timezone": 115,
	"mkt_clickid": 116,
	"mkt_network": 117,
	"etl_tags": 118,
	"dvce_sent_tstamp": 119,
	"refr_domain_userid": 120,
	"refr_device_tstamp": 121,
	"derived_contexts": 122,
	"domain_sessionid": 123,
	"derived_tstamp": 124,
	"event_vendor": 125,
	"event_name": 126,
	"event_format": 127,
	"event_version": 128,
	"event_fingerprint": 129,
	"true_tstamp": 130,
}

// Design decision: unstruct_event, contexts and derived_contexts return the structure `{"unstruct_event_com_acme_event_1": {"field1": "value1"}}`

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
			return nil, errors.New(fmt.Sprintf("Field %s is empty", field)) // Should this be an error?? Should we just return a zero value, or a nil perhaps?
		}
		kvPairs, err := enrichedEventFieldTypes[index].ParseFunction(enrichedEventFieldTypes[index].Key, record[index])
		if err != nil {
			return nil, err
		}
		if field == "contexts" || field == "derived_contexts" || field == "unstruct" {
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
	// TODO: Same error handling as above required.


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


/* DESIGN DECISION:

TransformToMap and TransfromToJson as it stands both ad geo_location by default.
geo_location is a field specific to elasticsearch mappings. It is normally an optional parameter defaulting to true.

For consistency this sdk should provide a means of including it but it shouldn't be mandatory.
Since optional parameters aren't a thing in Go, we have a couple of options:

Option 1:

Provide additional methods for this eg. TransformToMapWithGeo()
(feels like if going this route it's better to make the default one _not_ include it under the assumption that most won't actually want it)

Option 2:
Leave it as is, but the canonical way to transform _without_ the geo_location field is to use GetSubsetMap() or GetSubsetJson.
If going this route, we must decide how to do so - I reckon provide either []string{"all"} or an empty slice.
Empty slice has the downside that programmatic use of the function might end up accidentally transforming everything when we don't desire to transform anything...

Option 1 feels more idiomatic and intuitive tbh...

*/
