# GraphObs Neo4j Plugin – Procedure Reference

This document describes all stored procedures provided by the GraphObs Neo4j plugin.
Procedures are called from Cypher using `CALL <procedure>(<args>) YIELD <columns>`.

---

## Table of Contents

1. [Data Source Registration](#1-data-source-registration)
2. [Time Series Queries](#2-time-series-queries)
3. [Statistics](#3-statistics)
4. [Log Queries (OpenSearch)](#4-log-queries-opensearch)
5. [Trace Queries (Jaeger)](#5-trace-queries-jaeger)
6. [Span Queries (Jaeger)](#6-span-queries-jaeger)
7. [Jaeger Utilities](#7-jaeger-utilities)
8. [Aggregation over Many Nodes](#8-aggregation-over-many-nodes)
9. [Time Series Transformations](#9-time-series-transformations)
10. [Analysis – Change Detection](#10-analysis--change-detection)
11. [Analysis – Correlation & Causality](#11-analysis--correlation--causality)
12. [Analysis – Comparison & Hypothesis Tests](#12-analysis--comparison--hypothesis-tests)
13. [Graph Search](#13-graph-search)
14. [Temporal Search](#14-temporal-search)

---

## Common Parameter Patterns

### Time Window (`params` map)

Most data-retrieval procedures accept a `params` map. The following keys define the time window:

| Key | Type | Description |
|-----|------|-------------|
| `time` | String or Number | End time. ISO-8601 string or epoch milliseconds. Defaults to `now()`. |
| `range` | String | Relative lookback from `time`, e.g. `"-10m"`, `"-1h"`, `"-30s"`. Negative sign optional. |
| `startTime` | Number | Absolute window start (epoch ms). Overrides `time`+`range`. |
| `endTime` | Number | Absolute window end (epoch ms). Used together with `startTime`. |

Either use `time` + `range` (relative window) or `startTime` + `endTime` (absolute window).

### Return Types

| Type | Fields |
|------|--------|
| `TimeSeriesResult` | `timestamps: List<String>`, `values: Map<String, List<Double>>`, `source: String` |
| `StatisticResult` | `stats: Map<String, Double>` (keys: `min`, `max`, `mean`, `median`, `stddev`, `sum`, `count`, `percentile`) |
| `LogResult` | `timestamp: String`, `level: String`, `message: String`, `source: String` |
| `TraceResult` | `traceId: String`, `spans: List<Map>` |
| `SpanResult` | `traceId: String`, `spanId: String`, `operationName: String`, `startTime: Long`, `duration: Long`, `tags: Map<String,Object>`, `logs: List<Map>` |
| `ComparisonResult` | `meanA: Double`, `meanB: Double`, `pValue: Double`, `significant: Boolean`, `samplesA: Long`, `samplesB: Long` |
| `CorrelationResult` | `node: Node` (node group variant), `correlation: Double` |
| `IRFNodeResult` | `node: Node`, `responseOnA: List<Double>`, `responseOnB: List<Double>` |
| `IRFVectorResult` | `responseY1: List<Double>`, `responseY2: List<Double>` |
| `ServiceResult` | `service: Node` |
| `OperationResult` | `operation: Node` |
| `NodeResult` | `node: Node` |

---

## 1. Data Source Registration

### `graphobs.datasources.register_prometheus`

```cypher
CALL graphobs.datasources.register_prometheus(url, options)
YIELD message, url, podCount
```

Connects to a Prometheus instance, discovers pods and servers, builds the graph topology, and creates `:Prometheus`, `:Pod`, `:Server` nodes with `:HAS_TIME_SERIES` / `:RUNS_ON` relationships.

| Parameter | Type | Description |
|-----------|------|-------------|
| `url` | String | Prometheus base URL, e.g. `"http://localhost:9090"` |
| `options` | Map | Optional settings (see below) |

**`options` keys:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `update` | Boolean | `false` | If `true`, update existing nodes instead of skipping them |
| `intervalSeconds` | Number | `30` | Prometheus scrape step interval in seconds |

**YIELDS:** `message: String`, `url: String`, `podCount: Long`

---

### `graphobs.datasources.register_opensearch`

```cypher
CALL graphobs.datasources.register_opensearch(url, options)
YIELD message, url, indexCount
```

Connects to an OpenSearch instance, discovers indices, reads field mappings, and creates `:LogIndex` nodes with stored field metadata (`podField`, `serviceField`, `severityField`, `messageField`).

| Parameter | Type | Description |
|-----------|------|-------------|
| `url` | String | OpenSearch base URL, e.g. `"http://localhost:9200"` |
| `options` | Map | Optional settings (see below) |

**`options` keys:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `index` | String | `"*"` | Index name pattern to register |
| `user` | String | - | HTTP Basic Auth username |
| `password` | String | - | HTTP Basic Auth password |
| `update` | Boolean | `false` | If `true`, update existing `:LogIndex` nodes |
| `intervalSeconds` | Number | `60` | Polling interval (informational) |

**YIELDS:** `message: String`, `url: String`, `indexCount: Long`

---

### `graphobs.jaeger.import_graph`

```cypher
CALL graphobs.jaeger.import_graph(baseUrl)
YIELD message
```

Fetches all services and traces from Jaeger, imports them as `:Service` and `:Operation` nodes with `:DEPENDS_ON` relationships. Also detects and stores the pod tag key on the `:Jaeger` node.

| Parameter | Type | Description |
|-----------|------|-------------|
| `baseUrl` | String | Jaeger query API base URL, e.g. `"http://localhost:16686/api"` |

**YIELDS:** `message: String`

---

## 2. Time Series Queries

All procedures retrieve time series data from Prometheus (or locally stored data). The `tsName` parameter selects the metric name (e.g. `"cpu_usage"`, `"rps"`, `"latency_ms"`).

### `graphobs.data.get_time_series`

```cypher
CALL graphobs.data.get_time_series(node, tsName, params)
YIELD timestamps, values, source
```

Fetches the time series for a single graph node (Pod, Service, or Operation).

| Parameter | Type | Description |
|-----------|------|-------------|
| `node` | Node | A `:Pod`, `:Service`, or `:Operation` node |
| `tsName` | String | Metric name |
| `params` | Map | Time window + optional filters (see below) |

**`params` keys** (in addition to [Time Window](#common-parameter-patterns)):

| Key | Type | Description |
|-----|------|-------------|
| `aggregation` | String | Prometheus aggregation function, e.g. `"sum"`, `"avg"` |
| `period` | String | Step/resolution override |
| `pod` | String | Override pod name filter |
| `operation` | String | Override operation name filter |

---

### `graphobs.data.get_time_series_from_pod`

```cypher
CALL graphobs.data.get_time_series_from_pod(podServiceName, tsName, params)
YIELD timestamps, values, source
```

Looks up a `:Pod` node by its `name` property and fetches the time series. Same `params` as `get_time_series`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `podServiceName` | String | Value of the `name` property on the `:Pod` node |
| `tsName` | String | Metric name |
| `params` | Map | See `get_time_series` |

---

### `graphobs.data.get_time_series_from_names`

```cypher
CALL graphobs.data.get_time_series_from_names(names, tsName, params)
YIELD timestamps, values, source
```

Fetches time series for multiple named entities. Each name is looked up as a Pod or Service name.

| Parameter | Type | Description |
|-----------|------|-------------|
| `names` | List\<String\> | List of entity names |
| `tsName` | String | Metric name |
| `params` | Map | See `get_time_series` |

---

### `graphobs.data.get_time_series_batch`

```cypher
CALL graphobs.data.get_time_series_batch(nodes, tsName, params)
YIELD timestamps, values, source
```

Batch variant of `get_time_series` for a list of nodes. Uses a single Prometheus API call for all nodes when possible (same Prometheus instance), reducing HTTP overhead.

| Parameter | Type | Description |
|-----------|------|-------------|
| `nodes` | List\<Node\> | List of `:Pod`, `:Service`, or `:Operation` nodes |
| `tsName` | String | Metric name |
| `params` | Map | See `get_time_series` |

---

## 3. Statistics

Compute descriptive statistics over a time series.

**YIELDS** (all three procedures): `stats: Map<String, Double>` with keys:
`min`, `max`, `mean`, `median`, `stddev`, `sum`, `count`, `percentile` (default p95)

### `graphobs.data.get_statistic_from_node`

```cypher
CALL graphobs.data.get_statistic_from_node(node, tsName, params)
YIELD stats
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `node` | Node | A `:Pod`, `:Service`, or `:Operation` node |
| `tsName` | String | Metric name |
| `params` | Map | Time window + `percentile` (Number, default `95.0`) |

---

### `graphobs.data.get_statistic_from_pod`

```cypher
CALL graphobs.data.get_statistic_from_pod(podServiceName, tsName, params)
YIELD stats
```

Looks up a `:Pod` by `name` property. Same params as `get_statistic_from_node`.

---

### `graphobs.data.get_statistics_batch`

```cypher
CALL graphobs.data.get_statistics_batch(nodes, tsName, params)
YIELD stats
```

Batch variant for a list of nodes. Same params as `get_statistic_from_node`.

---

## 4. Log Queries (OpenSearch)

Queries OpenSearch for log entries. The `index` parameter selects the OpenSearch index pattern (e.g. `"otel-logs-*"`).

**YIELDS** (all three procedures): `timestamp: String`, `level: String`, `message: String`, `source: String`

### `graphobs.data.get_logs`

```cypher
CALL graphobs.data.get_logs(index, params)
YIELD timestamp, level, message, source
```

Retrieves logs from an OpenSearch index without filtering by entity.

| Parameter | Type | Description |
|-----------|------|-------------|
| `index` | String | OpenSearch index pattern |
| `params` | Map | Query parameters (see below) |

**`params` keys:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `time` | String/Number | now | End time (ISO-8601 or epoch ms) |
| `range` | String | `"-1h"` | Relative lookback, e.g. `"-10m"` |
| `limit` | Number | `100` | Maximum number of log entries to return |
| `level` | String | - | Filter by severity level, e.g. `"ERROR"`, `"WARN"` |
| `query` | String | - | Lucene query string filter |
| `dsl` | Map | - | Raw OpenSearch DSL query fragment to merge |
| `<any other key>` | String | - | Treated as a term filter on `<key>.keyword` field |

---

### `graphobs.data.get_logs_for`

```cypher
CALL graphobs.data.get_logs_for(node, index, params)
YIELD timestamp, level, message, source
```

Retrieves logs filtered by the given graph node (Pod, Service, or Operation).
The node type determines the OpenSearch filter field:
- `:Pod` → filters on pod name field (default `resource.k8s.pod.name.keyword`)
- `:Service` → filters on service name field (default `resource.service.name.keyword`)
- `:Operation` → extracts service name from operation ID, filters on service field

Stored field names from the `:LogIndex` node (set by registration) override the defaults.

| Parameter | Type | Description |
|-----------|------|-------------|
| `node` | Node | A `:Pod`, `:Service`, or `:Operation` node |
| `index` | String | OpenSearch index pattern |
| `params` | Map | Same as `get_logs` |

---

### `graphobs.data.get_logs_for_nodes`

```cypher
CALL graphobs.data.get_logs_for_nodes(nodes, index, params)
YIELD timestamp, level, message, source
```

Batch variant of `get_logs_for` for a list of nodes.

| Parameter | Type | Description |
|-----------|------|-------------|
| `nodes` | List\<Node\> | List of `:Pod`, `:Service`, or `:Operation` nodes |
| `index` | String | OpenSearch index pattern |
| `params` | Map | Same as `get_logs` |

---

## 5. Trace Queries (Jaeger)

Retrieve distributed traces from Jaeger. Traces are fetched live from the Jaeger HTTP API.

**YIELDS** (all four procedures): `traceId: String`, `spans: List<Map>`

### `graphobs.data.get_trace`

```cypher
CALL graphobs.data.get_trace(service, params)
YIELD traceId, spans
```

Fetches traces for a service by name string.

| Parameter | Type | Description |
|-----------|------|-------------|
| `service` | String | Service name as known to Jaeger |
| `params` | Map | Query parameters (see below) |

**`params` keys:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `time` | String/Number | now | End time |
| `range` | String | `"-1h"` | Relative lookback |
| `limit` | Number | `20` | Maximum number of traces |
| `operation` | String | - | Filter by operation name |
| `status_code` | String/Number | - | Filter by HTTP status code tag |

---

### `graphobs.data.get_traces_for_services`

```cypher
CALL graphobs.data.get_traces_for_services(services, params)
YIELD traceId, spans
```

Fetches traces for multiple service name strings. Same `params` as `get_trace`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `services` | List\<String\> | List of service names |
| `params` | Map | Same as `get_trace` |

---

### `graphobs.data.get_trace_for_node`

```cypher
CALL graphobs.data.get_trace_for_node(node, params)
YIELD traceId, spans
```

Resolves the service/pod name from a graph node and fetches matching traces.
Spans are filtered post-fetch to match the pod (using the pod tag key stored on the `:Jaeger` node, with fallback to `k8s.pod.name`, `pod.name`, `pod_name`, `kubernetes.pod.name`).

| Parameter | Type | Description |
|-----------|------|-------------|
| `node` | Node | A `:Pod`, `:Service`, or `:Operation` node |
| `params` | Map | Same as `get_trace` |

---

### `graphobs.data.get_traces_for_nodes`

```cypher
CALL graphobs.data.get_traces_for_nodes(nodes, params)
YIELD traceId, spans
```

Batch variant of `get_trace_for_node`. Deduplicates traces by `traceId` across all nodes.

| Parameter | Type | Description |
|-----------|------|-------------|
| `nodes` | List\<Node\> | List of `:Pod`, `:Service`, or `:Operation` nodes |
| `params` | Map | Same as `get_trace` |

---

## 6. Span Queries (Jaeger)

Retrieve individual spans (not full traces). Useful for operation-level analysis.

**YIELDS** (all three procedures): `traceId: String`, `spanId: String`, `operationName: String`, `startTime: Long`, `duration: Long`, `tags: Map<String,Object>`, `logs: List<Map>`

### `graphobs.data.get_spans`

```cypher
CALL graphobs.data.get_spans(service, params)
YIELD traceId, spanId, operationName, startTime, duration, tags, logs
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `service` | String | Service name |
| `params` | Map | Query parameters (see below) |

**`params` keys:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `time` | String/Number | now | End time |
| `range` | String | `"-1h"` | Relative lookback |
| `limit` | Number | `20` | Trace limit (spans are extracted from traces) |
| `operation` | String | - | Filter by operation name |
| `service` | String | - | Filter by service name (can differ from outer parameter) |
| `<any other key>` | String | - | Treated as a custom tag filter (exact match) |

---

### `graphobs.data.get_spans_for`

```cypher
CALL graphobs.data.get_spans_for(node, params)
YIELD traceId, spanId, operationName, startTime, duration, tags, logs
```

Resolves service/pod from node, fetches spans, and filters to spans belonging to the node.
Same `params` as `get_spans`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `node` | Node | A `:Pod`, `:Service`, or `:Operation` node |
| `params` | Map | Same as `get_spans` |

---

### `graphobs.data.get_spans_for_nodes`

```cypher
CALL graphobs.data.get_spans_for_nodes(nodes, params)
YIELD traceId, spanId, operationName, startTime, duration, tags, logs
```

Batch variant of `get_spans_for`. Same `params` as `get_spans`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `nodes` | List\<Node\> | List of nodes |
| `params` | Map | Same as `get_spans` |

---

## 7. Jaeger Utilities

### `graphobs.jaeger.call_count_between`

```cypher
CALL graphobs.jaeger.call_count_between(parent, child, params)
YIELD count
```

Counts how many times the parent service calls the child service within the time window.
Looks for traces where a span of `parent` has a child span of `child`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `parent` | Node | Calling `:Service` or `:Operation` node |
| `child` | Node | Called `:Service` or `:Operation` node |
| `params` | Map | Query parameters (see below) |

**`params` keys:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `time` | String/Number | now | End time |
| `lookback` | String | `"-1h"` | Relative lookback |
| `baseUrl` | String | from `:Jaeger` node | Override Jaeger API URL |

**YIELDS:** `count: Long`

---

### `graphobs.data.cypher_for_trace_subgraph_visualize`

```cypher
CALL graphobs.data.cypher_for_trace_subgraph_visualize(traceId)
YIELD cypherQuery
```

Returns a Cypher `MATCH` query string that can be used to visualize the subgraph corresponding to a specific trace ID in Neo4j Browser.

| Parameter | Type | Description |
|-----------|------|-------------|
| `traceId` | String | Jaeger trace ID |

**YIELDS:** `cypherQuery: String`

---

### `graphobs.data.get_traces_for_subgraph`

```cypher
CALL graphobs.data.get_traces_for_subgraph(params)
YIELD traceId, spans
```

Fetches traces that pass through a specific set of operations defined by nodes or names.

**`params` keys:**

| Key | Type | Description |
|-----|------|-------------|
| `operationNodes` | List\<Node\> | List of `:Operation` nodes to match |
| `operations` | List\<String\> | List of operation name strings (alternative to `operationNodes`) |
| `time` | String/Number | End time |
| `range` | String | Relative lookback |
| `limit` | Number | Trace limit |

---

## 8. Aggregation over Many Nodes

### `graphobs.data.get_all_for_service`

```cypher
CALL graphobs.data.get_all_for_service(node, metric, params)
YIELD timestamps, values, source
```

Aggregates a metric across all `:Pod` nodes connected to the given service node.
Uses Prometheus server-side aggregation when all pods share the same Prometheus instance (fast path), falls back to plugin-side aggregation otherwise.

Aggregation function by metric name:
- `rps`, `cpu_total` → **sum** across pods
- `latency_ms`, `error_rate_pct` → **average** across pods
- All other metrics → returned individually per pod (no aggregation)

| Parameter | Type | Description |
|-----------|------|-------------|
| `node` | Node | A `:Service` node (pods are discovered via `HAS_POD` relationships) |
| `metric` | String | Metric name |
| `params` | Map | Time window parameters |

---

### `graphobs.aggregation.aggregate_nodes`

```cypher
CALL graphobs.aggregation.aggregate_nodes(nodes, metric, params)
YIELD timestamps, values, source
```

Aggregates a metric over an arbitrary list of nodes. Uses Prometheus server-side aggregation when possible.

| Parameter | Type | Description |
|-----------|------|-------------|
| `nodes` | List\<Node\> | List of nodes |
| `metric` | String | Metric name |
| `params` | Map | Time window + `top_aggregation` key |

**Additional `params` key:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `top_aggregation` | String | `"sum"` | Aggregation function: `"sum"` or `"avg"` / `"average"` / `"mean"` |

---

## 9. Time Series Transformations

These procedures operate on pre-fetched time series data (lists of timestamps and values),
not on graph nodes directly. They are composable with `get_time_series` and similar procedures.

All `values` parameters have type `Map<String, List<Double>>` (metric name → value list).
All procedures **YIELD** `timestamps: List<String>`, `values: Map<String, List<Double>>`.

### `graphobs.aggregation.moving_average`

```cypher
CALL graphobs.aggregation.moving_average(timestamps, values, window)
YIELD timestamps, values
```

Computes a sliding window average over the value list.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `timestamps` | List\<String\> | - | Timestamp strings |
| `values` | Map\<String, List\<Double\>\> | - | Value series |
| `window` | Long | `5` | Window size (number of data points) |

Note: The output has `window - 1` fewer data points than the input.

---

### `graphobs.aggregation.difference`

```cypher
CALL graphobs.aggregation.difference(timestamps, values)
YIELD timestamps, values
```

Computes the difference between consecutive values: `v[i] - v[i-1]`.
Output has one fewer data point than input.

---

### `graphobs.aggregation.derivative`

```cypher
CALL graphobs.aggregation.derivative(timestamps, values)
YIELD timestamps, values
```

Computes the rate of change per second: `(v[i] - v[i-1]) / Δt_seconds`.
Output has one fewer data point than input.

---

### `graphobs.aggregation.cumulative_sum`

```cypher
CALL graphobs.aggregation.cumulative_sum(timestamps, values)
YIELD timestamps, values
```

Computes the running cumulative sum of the value series.

---

### `graphobs.aggregation.integral`

```cypher
CALL graphobs.aggregation.integral(timestamps, values)
YIELD timestamps, values
```

Computes the cumulative integral (area under the curve) using the trapezoidal rule: `Σ v[i] × Δt_seconds`.

---

### `graphobs.aggregation.linear_regression`

```cypher
CALL graphobs.aggregation.linear_regression(timestamps, values)
YIELD timestamps, values
```

Fits a linear model `ŷ = a·x + b` to the data and returns the fitted values at each timestamp point.

---

### `graphobs.aggregation.binned_average`

```cypher
CALL graphobs.aggregation.binned_average(timestamps, values, window)
YIELD timestamps, values
```

Divides the time range into bins of `window` seconds and returns the average value in each bin.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `timestamps` | List\<String\> | - | Timestamp strings |
| `values` | Map\<String, List\<Double\>\> | - | Value series |
| `window` | Long | `5` | Bin width in seconds |

---

## 10. Analysis – Change Detection

### `graphobs.analysis.pelt`

```cypher
CALL graphobs.analysis.pelt(timestamps, values, penalty)
YIELD changepoints
```

Detects changepoints in a time series using the PELT (Pruned Exact Linear Time) algorithm.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `timestamps` | List\<String\> | - | Timestamp strings |
| `values` | Map\<String, List\<Double\>\> | - | Value series (one per metric) |
| `penalty` | Double | - | Penalty value controlling sensitivity (higher = fewer changepoints) |

**YIELDS:** `changepoints: Map<String, List<String>>` - detected changepoint timestamps per metric.

---

### `graphobs.analysis.detect_regression_outliers`

```cypher
CALL graphobs.analysis.detect_regression_outliers(tsNode, metric, windowSize, residuumThreshold)
YIELD changepoints
```

Detects outliers in a stored time series node by fitting a local regression and flagging points where the residual exceeds the threshold.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tsNode` | Node | - | A node with stored time series properties |
| `metric` | String | - | Property name of the metric to analyse |
| `windowSize` | Long | - | Size of the rolling regression window |
| `residuumThreshold` | Double | - | Maximum allowed residual before a point is flagged |

**YIELDS:** `changepoints: Map<String, List<String>>` - flagged timestamps.

---

## 11. Analysis – Correlation & Causality

### `graphobs.analysis.correlation`

```cypher
CALL graphobs.analysis.correlation(timestamps1, values1, timestamps2, values2, params)
YIELD correlation
```

Computes the correlation between two pre-fetched time series.

| Parameter | Type | Description |
|-----------|------|-------------|
| `timestamps1` | List\<String\> | Timestamps for series 1 |
| `values1` | Map\<String, List\<Double\>\> | Values for series 1 |
| `timestamps2` | List\<String\> | Timestamps for series 2 |
| `values2` | Map\<String, List\<Double\>\> | Values for series 2 |
| `params` | Map | Options (see below) |

**`params` keys:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `method` | String | `"pearson"` | Correlation method: `"pearson"` or `"kendall"` |
| `join` | String | `"linear"` | Temporal alignment: `"linear"` (linear interpolation), `"resample"` (resample to regular grid), `"forwardfill"` |
| `intervalSeconds` | Number | `60` | Target interval in seconds for resampling/interpolation |

**YIELDS:** `correlation: Double`

---

### `graphobs.analysis.node_group_correlation`

```cypher
CALL graphobs.analysis.node_group_correlation(nodeA, metricA, nodesB, metricB, params)
YIELD node, correlation
```

Computes the correlation between a reference node's metric and each node in a group.
Fetches time series from Prometheus.

| Parameter | Type | Description |
|-----------|------|-------------|
| `nodeA` | Node | Reference node |
| `metricA` | String | Reference metric name |
| `nodesB` | List\<Node\> | Group of nodes to correlate against |
| `metricB` | String | Metric name for group nodes |
| `params` | Map | Options + time window |

**`params` keys** (in addition to [Time Window](#common-parameter-patterns)):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `method` | String | `"pearson"` | `"pearson"` or `"kendall"` |
| `join` | String | `"resample"` | Temporal alignment strategy |
| `intervalSeconds` | Number | `60` | Resampling interval in seconds |

**YIELDS:** `node: Node`, `correlation: Double`

---

### `graphobs.analysis.var_irf`

```cypher
CALL graphobs.analysis.var_irf(timestamps1, values1, timestamps2, values2, params)
YIELD responseY1, responseY2
```

Computes the Impulse Response Function (IRF) of a Vector Autoregression (VAR) model fitted to two pre-fetched time series.
The IRF describes how a unit shock to one series propagates to the other over time.

| Parameter | Type | Description |
|-----------|------|-------------|
| `timestamps1` | List\<String\> | Timestamps for series 1 |
| `values1` | Map\<String, List\<Double\>\> | Values for series 1 |
| `timestamps2` | List\<String\> | Timestamps for series 2 |
| `values2` | Map\<String, List\<Double\>\> | Values for series 2 |
| `params` | Map | Options (see below) |

**`params` keys:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `steps` | Number | `10` | Number of forecast steps for the impulse response |
| `join` | String | `"linear"` | Temporal alignment strategy |
| `intervalSeconds` | Number | `60` | Resampling interval |

**YIELDS:** `responseY1: List<Double>`, `responseY2: List<Double>`

---

### `graphobs.analysis.var_irf_node_group`

```cypher
CALL graphobs.analysis.var_irf_node_group(nodeA, metricA, nodesB, metricB, params)
YIELD node, responseOnA, responseOnB
```

Computes VAR IRF between a reference node and each node in a group.
Fetches time series from Prometheus.

| Parameter | Type | Description |
|-----------|------|-------------|
| `nodeA` | Node | Reference node |
| `metricA` | String | Reference metric |
| `nodesB` | List\<Node\> | Group of nodes |
| `metricB` | String | Group metric |
| `params` | Map | Options + time window |

**`params` keys** (in addition to [Time Window](#common-parameter-patterns)):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `intervalSeconds` | Number | `60` | Resampling interval |
| `steps` | Number | `10` | IRF steps |
| `join` | String | `"linear"` | Temporal alignment |

**YIELDS:** `node: Node`, `responseOnA: List<Double>`, `responseOnB: List<Double>`

---

### `graphobs.analysis.quantify_event_impact`

```cypher
CALL graphobs.analysis.quantify_event_impact(
    eventTime, timestamps, values,
    windowMinutes, baselineWindowMinutes, thresholdSigma)
YIELD property, peakImpact, impactDuration
```

Quantifies the impact of a specific event on a time series by comparing a post-event window against a baseline window.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `eventTime` | String/Long | - | Timestamp of the event (ISO-8601 or epoch ms) |
| `timestamps` | List\<String\> | - | Time series timestamps |
| `values` | Map\<String, List\<Double\>\> | - | Time series values |
| `windowMinutes` | Double | `10.0` | Duration of the post-event observation window (minutes) |
| `baselineWindowMinutes` | Double | `30.0` | Duration of the pre-event baseline window (minutes) |
| `thresholdSigma` | Double | `2.0` | Number of standard deviations above baseline mean to count as "impacted" |

**YIELDS:** `property: String`, `peakImpact: Double`, `impactDuration: Long` (number of impacted data points)

---

### `graphobs.analysis.correlate_events_with_value_by_id`

```cypher
CALL graphobs.analysis.correlate_events_with_value_by_id(
    eventNodeElementIds, eventTimestampProperty,
    valueNodeElementId, valueProperty, intervalSeconds)
YIELD correlation, contributingIntervals
```

Correlates a set of event nodes (discrete occurrences) with a continuous value time series using a binary event indicator aligned to the value series' time grid.

| Parameter | Type | Description |
|-----------|------|-------------|
| `eventNodeElementIds` | List\<String\> | Element IDs of event nodes |
| `eventTimestampProperty` | String | Property name holding the event timestamp |
| `valueNodeElementId` | String | Element ID of the node holding the value time series |
| `valueProperty` | String | Property name of the value metric |
| `intervalSeconds` | Long | Time grid interval in seconds for alignment |

**YIELDS:** `correlation: Double`, `contributingIntervals: Long`

---

## 12. Analysis – Comparison & Hypothesis Tests

All procedures yield a **`ComparisonResult`** with fields:
`meanA: Double`, `meanB: Double`, `pValue: Double`, `significant: Boolean`, `samplesA: Long`, `samplesB: Long`

### `graphobs.comparison.compare_ts_mean`

```cypher
CALL graphobs.comparison.compare_ts_mean(nodes, metric, options)
YIELD meanA, meanB, pValue, significant, samplesA, samplesB
```

Splits a list of nodes into two groups based on a criterion and compares their mean metric values using an independent two-sample t-test.

| Parameter | Type | Description |
|-----------|------|-------------|
| `nodes` | List\<Node\> | All nodes to compare |
| `metric` | String | Metric name |
| `options` | Map | Grouping criterion and options (see below) |

**`options` keys:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `property` | String | - | Property-based split, e.g. `"x > 10"`, `"region = 'us-east'"` |
| `label` | String | - | Label-based split: nodes with this label go to group A |
| `relation` | String | - | Cypher relationship pattern: nodes matching it go to group A |
| `alpha` | Double | `0.05` | Significance level |
| *(time window)* | - | - | Forwarded to `TimeSeriesUtil` |

---

### `graphobs.comparison.compare_two_groups`

```cypher
CALL graphobs.comparison.compare_two_groups(groupA, groupB, metric, params, options)
YIELD meanA, meanB, pValue, significant, samplesA, samplesB
```

Compares two explicitly provided groups using an independent two-sample t-test.

| Parameter | Type | Description |
|-----------|------|-------------|
| `groupA` | List\<Node\> | First group |
| `groupB` | List\<Node\> | Second group |
| `metric` | String | Metric name |
| `params` | Map | Time window parameters |
| `options` | Map | `alpha: Double` (default `0.05`) |

---

### `graphobs.comparison.compare_periods_means`

```cypher
CALL graphobs.comparison.compare_periods_means(nodes, metric, periodA, periodB, options)
YIELD meanA, meanB, pValue, significant, samplesA, samplesB
```

Compares per-node means across two time periods using an independent two-sample t-test.

| Parameter | Type | Description |
|-----------|------|-------------|
| `nodes` | List\<Node\> | Nodes to evaluate |
| `metric` | String | Metric name |
| `periodA` | Map | Time window for period A (same keys as [Time Window](#common-parameter-patterns)) |
| `periodB` | Map | Time window for period B |
| `options` | Map | `alpha: Double` (default `0.05`) |

---

### `graphobs.comparison.compare_periods_paired`

```cypher
CALL graphobs.comparison.compare_periods_paired(node, metric, timeParams, options)
YIELD meanA, meanB, pValue, significant, samplesA, samplesB
```

Compares two time periods for a single node using a **paired** t-test.
Both periods must contain the same number of data points.

| Parameter | Type | Description |
|-----------|------|-------------|
| `node` | Node | Single node |
| `metric` | String | Metric name |
| `timeParams` | Map | Period specification (see below) |
| `options` | Map | `alpha: Double` (default `0.05`) |

**`timeParams` formats** (two alternatives):

*Format 1 – Simplified:*

| Key | Type | Description |
|-----|------|-------------|
| `start1` | String/Number | Start of period A |
| `start2` | String/Number | Start of period B |
| `range` | String/Number | Duration of each period, e.g. `"-10m"` or milliseconds |

*Format 2 – Explicit:*

| Key | Type | Description |
|-----|------|-------------|
| `periodA` | Map | Standard time window map for period A |
| `periodB` | Map | Standard time window map for period B |

---

### `graphobs.comparison.compare_proportions`

```cypher
CALL graphobs.comparison.compare_proportions(
    node1, params1, node2, params2,
    trialMetric, eventMetric, options)
YIELD meanA, meanB, pValue, significant, samplesA, samplesB
```

Compares two proportions (event rate = events / trials) between two nodes using a two-proportion Z-test.

`meanA` and `meanB` represent the estimated proportions (between 0 and 1).

| Parameter | Type | Description |
|-----------|------|-------------|
| `node1` | Node | First node |
| `params1` | Map | Time window for node 1 |
| `node2` | Node | Second node |
| `params2` | Map | Time window for node 2 |
| `trialMetric` | String | Metric name for the trial count (denominator), e.g. `"rps"` |
| `eventMetric` | String | Metric name for the event rate (numerator), e.g. `"error_rate_pct"` |
| `options` | Map | Options (see below) |

**`options` keys:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `eventType` | String | `"relative"` | `"relative"`: eventMetric is a percentage (0–100); `"absolute"`: eventMetric is an absolute count |
| `alpha` | Double | `0.05` | Significance level |

---

## 13. Graph Search

All search procedures query the graph topology using `DEPENDS_ON` (operation dependencies) and `HAS_OPERATION` (service-operation ownership) relationships.

### Service-level Call Chain

#### `graphobs.search.get_calling_services`

```cypher
CALL graphobs.search.get_calling_services(serviceName, maxSteps)
YIELD service
```

Returns all services that (transitively) call the given service.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `serviceName` | String | - | Target service name |
| `maxSteps` | Long | `MAX_INT` | Maximum traversal depth |

---

#### `graphobs.search.get_called_services`

```cypher
CALL graphobs.search.get_called_services(serviceName, maxSteps)
YIELD service
```

Returns all services that the given service (transitively) calls.

---

#### `graphobs.search.get_calling_services_from_operation`

```cypher
CALL graphobs.search.get_calling_services_from_operation(serviceName, operationName, maxSteps)
YIELD service
```

Returns all services whose operations (transitively) depend on a specific operation.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `serviceName` | String | - | Service owning the target operation |
| `operationName` | String | - | Target operation name |
| `maxSteps` | Long | `MAX_INT` | Maximum traversal depth |

---

#### `graphobs.search.get_called_services_from_operation`

```cypher
CALL graphobs.search.get_called_services_from_operation(serviceName, operationName, maxSteps)
YIELD service
```

Returns all services whose operations are (transitively) called by a specific operation.

---

### Operation-level Call Chain

#### `graphobs.search.get_calling_operations_from_operation`

```cypher
CALL graphobs.search.get_calling_operations_from_operation(serviceName, operationName, maxSteps)
YIELD operation
```

Returns all operations that (transitively) call a specific operation.

---

#### `graphobs.search.get_called_operations_from_operation`

```cypher
CALL graphobs.search.get_called_operations_from_operation(serviceName, operationName, maxSteps)
YIELD operation
```

Returns all operations (transitively) called by a specific operation.

---

#### `graphobs.search.get_calling_operations_from_service`

```cypher
CALL graphobs.search.get_calling_operations_from_service(serviceName, maxSteps)
YIELD operation
```

Returns all operations that call any operation of the given service.

---

#### `graphobs.search.get_called_operations_from_service`

```cypher
CALL graphobs.search.get_called_operations_from_service(serviceName, maxSteps)
YIELD operation
```

Returns all operations called by any operation of the given service.

---

### Ownership Lookup

#### `graphobs.search.get_owning_service_by_name`

```cypher
CALL graphobs.search.get_owning_service_by_name(nodeName)
YIELD service
```

Finds the `:Service` that owns the node with the given name (Pod, Operation, etc.) by traversing upward via ownership relationships.

---

#### `graphobs.search.get_owning_service`

```cypher
CALL graphobs.search.get_owning_service(targetNode)
YIELD service
```

Finds the owning `:Service` for a specific node.

---

#### `graphobs.search.get_owning_services`

```cypher
CALL graphobs.search.get_owning_services(nodes)
YIELD service
```

Returns owning services for a list of nodes.

---

## 14. Temporal Search

These procedures search the graph for time-annotated nodes (`:event`, `:time_period`, `:time_series`) based on temporal relationships. All accept an optional `nodeElementIds` list to restrict the search to a specific set of nodes.

**Temporal node properties:**
- `:event` nodes have a `time` property
- `:time_period` and `:time_series` nodes have `start` and `end` properties

All temporal procedures **YIELD** `node: Node`.

### Overview Table

| Procedure | Input | Returns nodes that… |
|-----------|-------|---------------------|
| `before_by_object` | reference node | …end before reference starts |
| `after_by_object` | reference node | …start after reference ends |
| `within_by_object` | time window node | …are fully contained within window |
| `overlap_by_object` | time window node | …overlap with window |
| `outside_by_object` | time window node | …are fully outside window |
| `start_in_by_object` | time window node | …start within window |
| `end_in_by_object` | time window node | …end within window |
| `last_by_object` | reference node | …the single most recent node before reference |
| `next_by_object` | reference node | …the single next node after reference |
| `range_by_object` | reference node + duration + mode | …fall within reference ± duration |

### Object-Based Procedures (reference node as input)

#### `graphobs.time_search.before_by_object`

```cypher
CALL graphobs.time_search.before_by_object(referenceNode, nodeElementIds)
YIELD node
```

Returns all temporal nodes that end before the reference node starts.
Reference node must be `:event` (uses `time`) or `:time_period`/`:time_series` (uses `start`).

---

#### `graphobs.time_search.after_by_object`

```cypher
CALL graphobs.time_search.after_by_object(referenceNode, nodeElementIds)
YIELD node
```

Returns all temporal nodes that start after the reference node ends.

---

#### `graphobs.time_search.within_by_object`

```cypher
CALL graphobs.time_search.within_by_object(timeNode, nodeElementIds)
YIELD node
```

Returns all temporal nodes fully contained within the time window of `timeNode` (must have `start` and `end`).

---

#### `graphobs.time_search.overlap_by_object`

```cypher
CALL graphobs.time_search.overlap_by_object(timeNode, nodeElementIds)
YIELD node
```

Returns all temporal nodes that overlap with (partially or fully intersect) the time window of `timeNode`.

---

#### `graphobs.time_search.outside_by_object`

```cypher
CALL graphobs.time_search.outside_by_object(timeNode, nodeElementIds)
YIELD node
```

Returns all temporal nodes fully outside the time window of `timeNode`.

---

#### `graphobs.time_search.start_in_by_object`

```cypher
CALL graphobs.time_search.start_in_by_object(timeNode, nodeElementIds)
YIELD node
```

Returns all `:time_period` and `:time_series` nodes whose start falls within the window of `timeNode`.

---

#### `graphobs.time_search.end_in_by_object`

```cypher
CALL graphobs.time_search.end_in_by_object(timeNode, nodeElementIds)
YIELD node
```

Returns all `:time_period` and `:time_series` nodes whose end falls within the window of `timeNode`.

---

#### `graphobs.time_search.last_by_object`

```cypher
CALL graphobs.time_search.last_by_object(referenceNode, nodeElementIds)
YIELD node
```

Returns the single most recent temporal node that precedes the reference node (at most 1 result).

---

#### `graphobs.time_search.next_by_object`

```cypher
CALL graphobs.time_search.next_by_object(referenceNode, nodeElementIds)
YIELD node
```

Returns the single next temporal node following the reference node (at most 1 result).

---

#### `graphobs.time_search.range_by_object`

```cypher
CALL graphobs.time_search.range_by_object(referenceNode, range, mode, nodeElementIds)
YIELD node
```

Returns temporal nodes within a time window derived from the reference node by applying an ISO 8601 duration offset.

| Parameter | Type | Description |
|-----------|------|-------------|
| `referenceNode` | Node | Reference `:event`, `:time_period`, or `:time_series` node |
| `range` | String | ISO 8601 duration, e.g. `"PT30M"`, `"PT1H"` |
| `mode` | String | `"plus"` (extend end), `"minus"` (extend start), `"around"` (both directions) |
| `nodeElementIds` | List\<String\> | Optional filter list of element IDs |

---

### Value-Based Procedures (literal timestamps as input)

All value-based procedures take string timestamps as parameters instead of a reference node.
They otherwise mirror the object-based counterparts.

#### `graphobs.time_search.before_by_values`

```cypher
CALL graphobs.time_search.before_by_values(start, end, nodeElementIds)
YIELD node
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `start` | String | ISO-8601 reference start (used as reference time) |
| `end` | String | ISO-8601 reference end (optional, used if `start` not provided) |
| `nodeElementIds` | List\<String\> | Optional element ID filter |

---

#### `graphobs.time_search.after_by_values`

```cypher
CALL graphobs.time_search.after_by_values(start, end, nodeElementIds)
YIELD node
```

---

#### `graphobs.time_search.within_by_value`

```cypher
CALL graphobs.time_search.within_by_value(start, end, nodeElementIds)
YIELD node
```

---

#### `graphobs.time_search.overlap_by_value`

```cypher
CALL graphobs.time_search.overlap_by_value(start, end, nodeElementIds)
YIELD node
```

---

#### `graphobs.time_search.outside_by_value`

```cypher
CALL graphobs.time_search.outside_by_value(start, end, nodeElementIds)
YIELD node
```

---

#### `graphobs.time_search.start_in_by_value`

```cypher
CALL graphobs.time_search.start_in_by_value(start, end, nodeElementIds)
YIELD node
```

---

#### `graphobs.time_search.end_in_by_value`

```cypher
CALL graphobs.time_search.end_in_by_value(start, end, nodeElementIds)
YIELD node
```

---

#### `graphobs.time_search.last_by_value`

```cypher
CALL graphobs.time_search.last_by_value(start, end, nodeElementIds)
YIELD node
```

Returns the single most recent temporal node before the reference time (at most 1 result).

---

#### `graphobs.time_search.next_by_value`

```cypher
CALL graphobs.time_search.next_by_value(start, end, nodeElementIds)
YIELD node
```

Returns the single next temporal node after the reference time (at most 1 result).

---

#### `graphobs.time_search.range_by_values`

```cypher
CALL graphobs.time_search.range_by_values(centerTime, range, mode, nodeElementIds)
YIELD node
```

Returns temporal nodes within a window derived from `centerTime` ± a duration.

| Parameter | Type | Description |
|-----------|------|-------------|
| `centerTime` | String | ISO-8601 center timestamp |
| `range` | String | ISO 8601 duration, e.g. `"PT30M"` |
| `mode` | String | `"plus"`, `"minus"`, or `"around"` |
| `nodeElementIds` | List\<String\> | Optional element ID filter |
