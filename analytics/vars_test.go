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
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

var ctxt = `{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.acme/test_context/jsonschema/1-0-0","data":{"field1": 1}}, {"schema":"iglu:com.acme/test_context/jsonschema/1-0-0","data":{"field1": 2}}]}`
var invalidCtxt = `{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"fail","data":{"field1": 1}}]}`

var unstruct = `{"data":{"data":{"key":"value"},"schema":"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1"},"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0"}`
var invalidUnstruct = `{"data":{"data":{"key":"value"},"schema":"fail"},"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0"}`

var tstampValue, _ = time.Parse("2006-01-02 15:04:05.999", "2013-11-26 00:03:57.885")

var unstructString = `{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1","data":{"targetUrl":"http://www.example.com","elementClasses":["foreground"],"elementId":"exampleLink","unicodeTest":"<>angry_birds"}}}`

var contextsString = `{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:org.schema/WebPage/jsonschema/1-0-0","data":{"genre":"blog","inLanguage":"en-US","datePublished":"2014-11-06T00:00:00Z","author":"Fred Blundun","breadcrumb":["blog","releases"],"keywords":["snowplow","javascript","tracker","event"]}},{"schema":"iglu:org.w3/PerformanceTiming/jsonschema/1-0-0","data":{"navigationStart":1415358089861,"unloadEventStart":1415358090270,"unloadEventEnd":1415358090287,"redirectStart":0,"redirectEnd":0,"fetchStart":1415358089870,"domainLookupStart":1415358090102,"domainLookupEnd":1415358090102,"connectStart":1415358090103,"connectEnd":1415358090183,"requestStart":1415358090183,"responseStart":1415358090265,"responseEnd":1415358090265,"domLoading":1415358090270,"domInteractive":1415358090886,"domContentLoadedEventStart":1415358090968,"domContentLoadedEventEnd":1415358091309,"domComplete":0,"loadEventStart":0,"loadEventEnd":0}}]}`

var derivedContextsString = `{"schema":"iglu:com.snowplowanalytics.snowplow\/contexts\/jsonschema\/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow\/ua_parser_context\/jsonschema\/1-0-0","data":{"useragentFamily":"IE","useragentMajor":"7","useragentMinor":"0","useragentPatch":null,"useragentVersion":"IE 7.0","osFamily":"Windows XP","osMajor":null,"osMinor":null,"osPatch":null,"osPatchMinor":null,"osVersion":"Windows XP","deviceFamily":"Other"}}]}`

// full event slice
var fullEvent = ParsedEvent([]string{
	"<>angry-birds",
	"web",
	"2013-11-26 00:03:57.885",
	"2013-11-26 00:03:57.885",
	"2013-11-26 00:03:57.885",
	"page_view",
	"c6ef3124-b53a-4b13-a233-0088f79dcbcb",
	"41828",
	"cloudfront-1",
	"js-2.1.0",
	"clj-tomcat-0.1.0",
	"serde-0.5.2",
	"jon.doe@email.com",
	"92.231.54.234",
	"2161814971",
	"bc2e92ec6c204a14",
	"3",
	"ecdff4d0-9175-40ac-a8bb-325c49733607",
	"US",
	"TX",
	"New York",
	"94109",
	"37.443604",
	"-122.4124",
	"Florida",
	"FDN Communications",
	"Bouygues Telecom",
	"nuvox.net",
	"Cable/DSL",
	"http://www.snowplowanalytics.com",
	"On Analytics",
	"",
	"http",
	"www.snowplowanalytics.com",
	"80",
	"/product/index.html",
	"id=GTM-DLRG",
	"4-conclusion",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	contextsString,
	"",
	"",
	"",
	"",
	"",
	unstructString,
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"1",
	"0",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	"",
	derivedContextsString,
	"2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
	"2013-11-26 00:03:57.885",
	"com.snowplowanalytics.snowplow",
	"link_click",
	"jsonschema",
	"1-0-0",
	"e3dbfa9cca0412c3d4052863cefb547f",
	"2013-11-26 00:03:57.885",
})

// tsv string
var tsvEvent = strings.Join(fullEvent, "\t")

var eventMapWithGeoJSON = []byte(`{"app_id":"<>angry-birds","br_features_flash":false,"br_features_pdf":true,"collector_tstamp":"2013-11-26T00:03:57.885Z","contexts_com_snowplowanalytics_snowplow_ua_parser_context_1":[{"deviceFamily":"Other","osFamily":"Windows XP","osMajor":null,"osMinor":null,"osPatch":null,"osPatchMinor":null,"osVersion":"Windows XP","useragentFamily":"IE","useragentMajor":"7","useragentMinor":"0","useragentPatch":null,"useragentVersion":"IE 7.0"}],"contexts_org_schema_web_page_1":[{"author":"Fred Blundun","breadcrumb":["blog","releases"],"datePublished":"2014-11-06T00:00:00Z","genre":"blog","inLanguage":"en-US","keywords":["snowplow","javascript","tracker","event"]}],"contexts_org_w3_performance_timing_1":[{"connectEnd":1415358090183,"connectStart":1415358090103,"domComplete":0,"domContentLoadedEventEnd":1415358091309,"domContentLoadedEventStart":1415358090968,"domInteractive":1415358090886,"domLoading":1415358090270,"domainLookupEnd":1415358090102,"domainLookupStart":1415358090102,"fetchStart":1415358089870,"loadEventEnd":0,"loadEventStart":0,"navigationStart":1415358089861,"redirectEnd":0,"redirectStart":0,"requestStart":1415358090183,"responseEnd":1415358090265,"responseStart":1415358090265,"unloadEventEnd":1415358090287,"unloadEventStart":1415358090270}],"derived_tstamp":"2013-11-26T00:03:57.885Z","domain_sessionid":"2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1","domain_sessionidx":3,"domain_userid":"bc2e92ec6c204a14","dvce_created_tstamp":"2013-11-26T00:03:57.885Z","etl_tstamp":"2013-11-26T00:03:57.885Z","event":"page_view","event_fingerprint":"e3dbfa9cca0412c3d4052863cefb547f","event_format":"jsonschema","event_id":"c6ef3124-b53a-4b13-a233-0088f79dcbcb","event_name":"link_click","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","geo_city":"New York","geo_country":"US","geo_latitude":37.443604,"geo_location":"37.443604,-122.4124","geo_longitude":-122.4124,"geo_region":"TX","geo_region_name":"Florida","geo_zipcode":"94109","ip_domain":"nuvox.net","ip_isp":"FDN Communications","ip_netspeed":"Cable/DSL","ip_organization":"Bouygues Telecom","name_tracker":"cloudfront-1","network_userid":"ecdff4d0-9175-40ac-a8bb-325c49733607","page_title":"On Analytics","page_url":"http://www.snowplowanalytics.com","page_urlfragment":"4-conclusion","page_urlhost":"www.snowplowanalytics.com","page_urlpath":"/product/index.html","page_urlport":80,"page_urlquery":"id=GTM-DLRG","page_urlscheme":"http","platform":"web","true_tstamp":"2013-11-26T00:03:57.885Z","txn_id":41828,"unstruct_event_com_snowplowanalytics_snowplow_link_click_1":{"elementClasses":["foreground"],"elementId":"exampleLink","targetUrl":"http://www.example.com","unicodeTest":"<>angry_birds"},"user_fingerprint":"2161814971","user_id":"jon.doe@email.com","user_ipaddress":"92.231.54.234","v_collector":"clj-tomcat-0.1.0","v_etl":"serde-0.5.2","v_tracker":"js-2.1.0"}`)
var eventMapWithoutGeoJSON = []byte(`{"app_id":"<>angry-birds","br_features_flash":false,"br_features_pdf":true,"collector_tstamp":"2013-11-26T00:03:57.885Z","contexts_com_snowplowanalytics_snowplow_ua_parser_context_1":[{"deviceFamily":"Other","osFamily":"Windows XP","osMajor":null,"osMinor":null,"osPatch":null,"osPatchMinor":null,"osVersion":"Windows XP","useragentFamily":"IE","useragentMajor":"7","useragentMinor":"0","useragentPatch":null,"useragentVersion":"IE 7.0"}],"contexts_org_schema_web_page_1":[{"author":"Fred Blundun","breadcrumb":["blog","releases"],"datePublished":"2014-11-06T00:00:00Z","genre":"blog","inLanguage":"en-US","keywords":["snowplow","javascript","tracker","event"]}],"contexts_org_w3_performance_timing_1":[{"connectEnd":1415358090183,"connectStart":1415358090103,"domComplete":0,"domContentLoadedEventEnd":1415358091309,"domContentLoadedEventStart":1415358090968,"domInteractive":1415358090886,"domLoading":1415358090270,"domainLookupEnd":1415358090102,"domainLookupStart":1415358090102,"fetchStart":1415358089870,"loadEventEnd":0,"loadEventStart":0,"navigationStart":1415358089861,"redirectEnd":0,"redirectStart":0,"requestStart":1415358090183,"responseEnd":1415358090265,"responseStart":1415358090265,"unloadEventEnd":1415358090287,"unloadEventStart":1415358090270}],"derived_tstamp":"2013-11-26T00:03:57.885Z","domain_sessionid":"2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1","domain_sessionidx":3,"domain_userid":"bc2e92ec6c204a14","dvce_created_tstamp":"2013-11-26T00:03:57.885Z","etl_tstamp":"2013-11-26T00:03:57.885Z","event":"page_view","event_fingerprint":"e3dbfa9cca0412c3d4052863cefb547f","event_format":"jsonschema","event_id":"c6ef3124-b53a-4b13-a233-0088f79dcbcb","event_name":"link_click","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","geo_city":"New York","geo_country":"US","geo_latitude":37.443604,"geo_longitude":-122.4124,"geo_region":"TX","geo_region_name":"Florida","geo_zipcode":"94109","ip_domain":"nuvox.net","ip_isp":"FDN Communications","ip_netspeed":"Cable/DSL","ip_organization":"Bouygues Telecom","name_tracker":"cloudfront-1","network_userid":"ecdff4d0-9175-40ac-a8bb-325c49733607","page_title":"On Analytics","page_url":"http://www.snowplowanalytics.com","page_urlfragment":"4-conclusion","page_urlhost":"www.snowplowanalytics.com","page_urlpath":"/product/index.html","page_urlport":80,"page_urlquery":"id=GTM-DLRG","page_urlscheme":"http","platform":"web","true_tstamp":"2013-11-26T00:03:57.885Z","txn_id":41828,"unstruct_event_com_snowplowanalytics_snowplow_link_click_1":{"elementClasses":["foreground"],"elementId":"exampleLink","targetUrl":"http://www.example.com","unicodeTest":"<>angry_birds"},"user_fingerprint":"2161814971","user_id":"jon.doe@email.com","user_ipaddress":"92.231.54.234","v_collector":"clj-tomcat-0.1.0","v_etl":"serde-0.5.2","v_tracker":"js-2.1.0"}`)

var eventMapWithGeo = map[string]interface{}{
	"app_id":            "<>angry-birds",
	"br_features_flash": false,
	"br_features_pdf":   true,
	"collector_tstamp":  tstampValue,
	"contexts_com_snowplowanalytics_snowplow_ua_parser_context_1": []interface{}{map[string]interface{}{
		"deviceFamily":     "Other",
		"osFamily":         "Windows XP",
		"osMajor":          interface{}(nil),
		"osMinor":          interface{}(nil),
		"osPatch":          interface{}(nil),
		"osPatchMinor":     interface{}(nil),
		"osVersion":        "Windows XP",
		"useragentFamily":  "IE",
		"useragentMajor":   "7",
		"useragentMinor":   "0",
		"useragentPatch":   interface{}(nil),
		"useragentVersion": "IE 7.0",
	}},
	"contexts_org_schema_web_page_1": []interface{}{map[string]interface{}{
		"author":        "Fred Blundun",
		"breadcrumb":    []interface{}{"blog", "releases"},
		"datePublished": "2014-11-06T00:00:00Z",
		"genre":         "blog",
		"inLanguage":    "en-US",
		"keywords":      []interface{}{"snowplow", "javascript", "tracker", "event"},
	}},
	"contexts_org_w3_performance_timing_1": []interface{}{map[string]interface{}{
		"connectEnd":                 1.415358090183e+12,
		"connectStart":               1.415358090103e+12,
		"domComplete":                0.0,
		"domContentLoadedEventEnd":   1.415358091309e+12,
		"domContentLoadedEventStart": 1.415358090968e+12,
		"domInteractive":             1.415358090886e+12,
		"domLoading":                 1.41535809027e+12,
		"domainLookupEnd":            1.415358090102e+12,
		"domainLookupStart":          1.415358090102e+12,
		"fetchStart":                 1.41535808987e+12,
		"loadEventEnd":               0.0,
		"loadEventStart":             0.0,
		"navigationStart":            1.415358089861e+12,
		"redirectEnd":                0.0,
		"redirectStart":              0.0,
		"requestStart":               1.415358090183e+12,
		"responseEnd":                1.415358090265e+12,
		"responseStart":              1.415358090265e+12,
		"unloadEventEnd":             1.415358090287e+12,
		"unloadEventStart":           1.41535809027e+12,
	}},
	"derived_tstamp":      tstampValue,
	"domain_sessionid":    "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
	"domain_sessionidx":   3,
	"domain_userid":       "bc2e92ec6c204a14",
	"dvce_created_tstamp": tstampValue,
	"etl_tstamp":          tstampValue,
	"event":               "page_view",
	"event_fingerprint":   "e3dbfa9cca0412c3d4052863cefb547f",
	"event_format":        "jsonschema",
	"event_id":            "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
	"event_name":          "link_click",
	"event_vendor":        "com.snowplowanalytics.snowplow",
	"event_version":       "1-0-0",
	"geo_city":            "New York",
	"geo_country":         "US",
	"geo_latitude":        37.443604,
	"geo_location":        "37.443604,-122.4124",
	"geo_longitude":       -122.4124,
	"geo_region":          "TX",
	"geo_region_name":     "Florida",
	"geo_zipcode":         "94109",
	"ip_domain":           "nuvox.net",
	"ip_isp":              "FDN Communications",
	"ip_netspeed":         "Cable/DSL",
	"ip_organization":     "Bouygues Telecom",
	"name_tracker":        "cloudfront-1",
	"network_userid":      "ecdff4d0-9175-40ac-a8bb-325c49733607",
	"page_title":          "On Analytics",
	"page_url":            "http://www.snowplowanalytics.com",
	"page_urlfragment":    "4-conclusion",
	"page_urlhost":        "www.snowplowanalytics.com",
	"page_urlpath":        "/product/index.html",
	"page_urlport":        80,
	"page_urlquery":       "id=GTM-DLRG",
	"page_urlscheme":      "http",
	"platform":            "web",
	"true_tstamp":         tstampValue,
	"txn_id":              41828,
	"unstruct_event_com_snowplowanalytics_snowplow_link_click_1": map[string]interface{}{
		"elementClasses": []interface{}{"foreground"},
		"elementId":      "exampleLink",
		"targetUrl":      "http://www.example.com",
		"unicodeTest":    "<>angry_birds",
	},
	"user_fingerprint": "2161814971",
	"user_id":          "jon.doe@email.com",
	"user_ipaddress":   "92.231.54.234",
	"v_collector":      "clj-tomcat-0.1.0",
	"v_etl":            "serde-0.5.2",
	"v_tracker":        "js-2.1.0",
}

func copyWithoutGeo(inMap map[string]interface{}) map[string]interface{} {
	outMap := make(map[string]interface{})
	for key, value := range inMap {
		if key != "geo_location" {
			outMap[key] = value
		}
	}
	return outMap
}

var eventMapWithoutGeo = copyWithoutGeo(eventMapWithGeo)

var unstructMap = map[string]interface{}{"unstruct_event_com_snowplowanalytics_snowplow_link_click_1": eventMapWithGeo["unstruct_event_com_snowplowanalytics_snowplow_link_click_1"]}

var contextsArray = []interface{}{"blog"}

var multipleContextsMap = map[string]interface{}{"contexts_org_schema_web_page_1": []interface{}{map[string]interface{}{"author": "Fred Blundun", "breadcrumb": []interface{}{"blog", "releases"}, "datePublished": "2014-11-06T00:00:00Z", "genre": "blog", "inLanguage": "en-US", "keywords": []interface{}{"snowplow", "javascript", "tracker", "event"}}}, "contexts_org_w3_performance_timing_1": []interface{}{map[string]interface{}{"connectEnd": 1.415358090183e+12, "connectStart": 1.415358090103e+12, "domComplete": 0.0, "domContentLoadedEventEnd": 1.415358091309e+12, "domContentLoadedEventStart": 1.415358090968e+12, "domInteractive": 1.415358090886e+12, "domLoading": 1.41535809027e+12, "domainLookupEnd": 1.415358090102e+12, "domainLookupStart": 1.415358090102e+12, "fetchStart": 1.41535808987e+12, "loadEventEnd": 0.0, "loadEventStart": 0.0, "navigationStart": 1.415358089861e+12, "redirectEnd": 0.0, "redirectStart": 0.0, "requestStart": 1.415358090183e+12, "responseEnd": 1.415358090265e+12, "responseStart": 1.415358090265e+12, "unloadEventEnd": 1.415358090287e+12, "unloadEventStart": 1.41535809027e+12}}}

var wholeContextMap = []interface{}{map[string]interface{}{"author": "Fred Blundun", "breadcrumb": []interface{}{"blog", "releases"}, "datePublished": "2014-11-06T00:00:00Z", "genre": "blog", "inLanguage": "en-US", "keywords": []interface{}{"snowplow", "javascript", "tracker", "event"}}}

var subsetMap = map[string]interface{}{
	"app_id":            eventMapWithGeo["app_id"],
	"br_features_flash": eventMapWithGeo["br_features_flash"],
	"br_features_pdf":   eventMapWithGeo["br_features_pdf"],
	"collector_tstamp":  tstampValue,
	"unstruct_event_com_snowplowanalytics_snowplow_link_click_1":  eventMapWithGeo["unstruct_event_com_snowplowanalytics_snowplow_link_click_1"],
	"contexts_org_w3_performance_timing_1":                        eventMapWithGeo["contexts_org_w3_performance_timing_1"],
	"contexts_org_schema_web_page_1":                              eventMapWithGeo["contexts_org_schema_web_page_1"],
	"contexts_com_snowplowanalytics_snowplow_ua_parser_context_1": eventMapWithGeo["contexts_com_snowplowanalytics_snowplow_ua_parser_context_1"],
}

var subsetJson, _ = jsoniter.Marshal(subsetMap)
