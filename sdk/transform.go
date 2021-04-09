package sdk

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
				kVPair, err := knownFields[index].ParseFunction(knownFields[index].Key, value)
				if err != nil {
					return nil, err
				}
				// append all results
				for _, pair := range kVPair {
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
