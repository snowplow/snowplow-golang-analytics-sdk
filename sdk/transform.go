package sdk

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
  "encoding/csv"
  "strings"
	// "github.com/pkg/errors"


	//	"unicode" // For camel to snake case - consider alternative?
  //	"errors" // TODO: Decide what to do for error handling
	// 	"github.com/hashicorp/go-multierror"
	//	"github.com/pkg/errors"
)

// Could use a slice/array instead maybe? Pehaps give it a test - maybe no point
// Should be named KeyVal not KeyVals
type KeyVals struct {
	Key   string
	Value interface{}
}

type ValueParser func(string, string) []KeyVals

type KeyFunctionPair struct {
	Key  string
	Func ValueParser
}

// TODO: CHECK IF WE EVEN NEED TO DO THESE TSTAMP FUNCTIONS - OTHER SDKS JUST HANDLE IT AS A STRING
// Is parse the correct nomenclature?
// rename to parseTstamp?
func parseNullableTime(timeString string) *time.Time {
	timeLayout := "2006-01-02 15:04:05.999"
	res, _ := time.Parse(timeLayout, timeString)
	if time.Time.IsZero(res) {
		return nil
	} else {
		return &res
	}
}

// These should all probably return KeyVals, err
// Also perhaps they should all return slices - because the custom contexts one returns arbitrary length...
func parseTime(key string, value string) []KeyVals {
	return []KeyVals{KeyVals{key, parseNullableTime(value)}}
}

func parseString(key string, value string) []KeyVals {
	return []KeyVals{KeyVals{key, value}}
}

func parseInt(key string, value string) []KeyVals {
	intvalue, err := strconv.Atoi(value)
	if err != nil {
		fmt.Println(err)
	}
	return []KeyVals{KeyVals{key, intvalue}}
}

func parseBool(key string, value string) []KeyVals {
	boolvalue, err := strconv.ParseBool(value)
	if err != nil {
		fmt.Println(err)
	}
	return []KeyVals{KeyVals{key, boolvalue}}
}

func parseDouble(key string, value string) []KeyVals {
	doubleValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		fmt.Println(err)
	}
	return []KeyVals{KeyVals{key, doubleValue}}
}

func parseContexts(key string, value string) []KeyVals {
	out, _ := shredContexts(value)
	return out // TODO: FIX THIS BY CHANGING ALL PARSERS TO RETURN ERRORS ALSO
}

func parseUnstruct(key string, value string) []KeyVals {
	out, _ := shredUnstruct(value)
	return out // TODO: FIX THIS BY CHANGING ALL PARSERS TO RETURN ERRORS ALSO
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

// CSV string to be parsed before this function.
// Maybe this should be goodEventToMap
// event is slice because csv package outputs a slice.
// TODO: figure out how to make it a fixed-length array.
func jsonifyGoodEvent(event []string, knownFields [131]KeyFunctionPair, addGeolocationData bool) []byte {

  if len(event) != len(knownFields) {
    fmt.Println("Wrong number of fields")
  } else {
    output := make(map[string]interface{})
    if addGeolocationData && event[latitudeIndex] != "" && event[longitudeIndex] != "" {
      output["geo_location"] = event[latitudeIndex] + "," + event[longitudeIndex]
    }
    for index, value := range event {
      // skip if empty
      if event[index] != "" {
        // apply function if not empty
        kVPair := knownFields[index].Func(knownFields[index].Key, value)
        // append all results
        for _, pair := range kVPair {
          output[pair.Key] = pair.Value
        }
      }
    }
    jsonOutput, err := json.Marshal(output)
    if err != nil {
      fmt.Println(err)
    }
    return jsonOutput
  }
  // TODO: Sort return value for unhappy path
  // TODO: Figure out how to split everything up into sensible functions
  return nil
}

// Since Golang tries its hardest to design against optional parameters, electing to implement the main Transform function
// to mirror the most common usage of the function - with addGeolocationData set to true (ie the default in the other SDKs).
// TODO: Design decisions to be made around what other functions to implement/expose - or how to approach doing other things
// One option: A method to transform only a specific set of atomic fields - which can be configured to transform all fields without doing the golocation bit...
func Transform(event string) []byte {

  // I think I prefer to just strings.Split("/t") if we can get away with it. Removes an import, and removes the need to needlessly memory on this reader object too/

  r := csv.NewReader(strings.NewReader(event))
	r.Comma = '\t'
	r.LazyQuotes = true

	record, err := r.Read()
	if err != nil {
		fmt.Println(err)
	}

  return jsonifyGoodEvent(record, enrichedEventFieldTypes, true)
}
