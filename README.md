# Snowplow Golang Analytics SDK

[![Build Status][gh-actions-image]][gh-actions] [![Release][release-image]][releases] [![License][license-image]][license]

![snowplow-logo](media/snowplow_logo.png)

Snowplow is a scalable open-source platform for rich, high quality, low-latency data collection. It is designed to collect high quality, complete behavioural data for enterprise business.

## Snowplow Pipeline Overview

![snowplow-pipeline](media/snowplow_architecture.png)

The [Snowplow trackers][tracker-docs] enable highly customisable collection of raw, unopinionated event data. The pipeline validates these events against a JSONSchema - to guarantee a high quality dataset - and adds information via both standard and custom enrichments.

This data is then made available in-stream for real-time processing, and can also be loaded to blob storage and data warehouse for analysis.

The Snowplow atomic data acts as an immutable log of all the actions that occurred across your digital products. The [analytics SDKs][sdk-docs] are libraries in a range languages which facilitate working with Snowplow Enriched data, by transforming it from its original TSV format to a more amenable format for programmatic interaction - for example JSON.

## Try Snowplow

This repo contains an analytics SDK which is relevant to users who already have a full Snowplow pipeline running (which can be done Open Source or via our [Snowplow Insights](https://snowplowanalytics.com/snowplow-insights/) service).

If you don't have a pipeline yet, you might be interested in finding out what Snowplow can do by setting up [Try Snowplow](https://try.snowplowanalytics.com/?utm_source=github&utm_medium=post&utm_campaign=try-snowplow).

## Quickstart

```bash
go get github.com/snowplow/snowplow-golang-analytics-sdk
```

```go
import "github.com/snowplow/snowplow-golang-analytics-sdk/analytics"

parsed, err := ParseEvent(event) // Where event is a valid TSV string Snowplow event.
if err != nil {
  fmt.Println(err)
}

parsed.ToJson() // whole event to JSON
parsed.ToMap() // whole event to map
parsed.GetValue("page_url") // get a value for a single canonical field
parsed.GetSubsetMap("page_url", "domain_userid", "contexts", "derived_contexts") // Get a map of values for a set of canonical fields
parsed.GetSubsetJson("page_url", "unstruct_event") // Get a JSON of values for a set of canonical fields
```

## API

```go
func ParseEvent(event string) (ParsedEvent, error)
```

ParseEvent takes a Snowplow Enriched event tsv string as input, and returns a 'ParsedEvent' typed slice of strings.
Methods may then be called on the resulting ParsedEvent type to transform the event, or a subset of the event to Map or Json.

```go
func (event ParsedEvent) ToJson() ([]byte, error)
```

ToJson transforms a valid Snowplow ParsedEvent to a JSON object.

```go
func (event ParsedEvent) ToMap() (map[string]interface{}, error)
```

ToMap transforms a valid Snowplow ParsedEvent to a Go map.

```go
func (event ParsedEvent) GetSubsetJson(fields ...string) ([]byte, error)
```

GetSubsetJson returns a JSON object containing a subset of the event, containing only the atomic fields provided, without processing the rest of the event.
For custom events and contexts, only "unstruct_event", "contexts", or "derived_contexts" may be provided, which will produce the entire data object for that field.
For contexts, the resultant map will contain all occurrences of all contexts within the provided field.

```go
func (event ParsedEvent) GetSubsetMap(fields ...string) (map[string]interface{}, error)
```

GetSubsetMap returns a map of a subset of the event, containing only the atomic fields provided, without processing the rest of the event.
For custom events and contexts, only "unstruct_event", "contexts", or "derived_contexts" may be provided, which will produce the entire data object for that field.
For contexts, the resultant map will contain all occurrences of all contexts within the provided field.

```go
func (event ParsedEvent) GetValue(field string) (interface{}, error)
```

GetValue returns the value for a provided atomic field, without processing the rest of the event.
For unstruct_event, it returns a map of only the data for the unstruct event.

```go
func (event ParsedEvent) ToJsonWithGeo() ([]byte, error)
```

ToJsonWithGeo adds the geo_location field, and transforms a valid Snowplow ParsedEvent to a JSON object.

```go
func (event ParsedEvent) ToMapWithGeo() (map[string]interface{}, error)
```

ToMapWithGeo adds the geo_location field, and transforms a valid Snowplow ParsedEvent to a Go map.

## Copyright and license

SQL Runner is copyright 2021 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[gh-actions]: https://github.com/snowplow/snowplow-golang-analytics-sdk/actions
[gh-actions-image]: https://github.com/snowplow/snowplow-golang-analytics-sdk/workflows/Test/badge.svg?branch=master

[release-image]: https://img.shields.io/github/v/release/snowplow/snowplow-golang-tracker?include_prereleases
[releases]: https://img.shields.io/github/v/release/snowplow/snowplow-golang-tracker

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[tracker-docs]: https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/
[sdk-docs]: https://docs.snowplowanalytics.com/docs/modeling-your-data/analytics-sdk/
