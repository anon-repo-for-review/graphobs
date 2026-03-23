# Optimierungsvorschlaege

## Teil A: Batch-Query-Optimierungen

Viele Procedures fuehren **pro Knoten einzelne HTTP-Requests** gegen externe Datenquellen aus. Bei Listen von Knoten entsteht ein N+1-artiges Problem: N Knoten = N sequenzielle HTTP-Calls.

### A1. Prometheus-Registrierung: Batch-PromQL statt N Einzel-Queries

**Betroffene Klassen:**
- `PrometheusRegistrationService.register()` — 3 HTTP-Calls pro Pod (55 Pods = ~165 Calls)
- `PrometheusToNeo4jUpdater.synchronizeOnce()` — 1 Call pro Pod fuer lastSeen (55 Pods = ~55 Calls)
- `PrometheusHttpClient.getPodTimeRange()` — ruft pro Pod: `getPrometheusTime()` + `kube_pod_created{pod=X}` + Fallback `min_over_time` + `max_over_time`
- `PrometheusHttpClient.getServerTimeRange()` — 3 Calls pro Server

**Problem:** Bei 55 Pods: ~180 sequenzielle HTTP-Requests = ~27s + 10s Sleep = ~37s.

**Loesung:** PromQL kann Daten fuer **alle Entitaeten gleichzeitig** liefern:

```promql
-- Statt 55x einzeln:
kube_pod_created{pod="pod-1"}
kube_pod_created{pod="pod-2"}
...

-- Einmal fuer alle:
kube_pod_created
-- Liefert: [{metric: {pod: "pod-1"}, value: [ts, val]}, {metric: {pod: "pod-2"}, ...}, ...]
```

Konkret:

| Aktuell (pro Pod) | Batch-Alternative | Ersparnis |
|---|---|---|
| `kube_pod_created{pod="X"}` x55 | `kube_pod_created` (1 Query) | 54 Calls |
| `min_over_time(container_last_seen{pod="X"}[365d])` x55 | `min_over_time(container_last_seen[365d])` (1 Query, gruppiert nach Pod-Label) | 54 Calls |
| `max_over_time(container_last_seen{pod="X"}[365d])` x55 | `max_over_time(container_last_seen[365d])` (1 Query) | 54 Calls |
| `getPrometheusTime()` x55+ | 1x cachen pro register()/synchronizeOnce() | ~55 Calls |

**Geschaetzte Verbesserung:** ~180 Calls -> ~5 Calls. **~40s -> ~2-3s.**

Fuer den Updater: `max_over_time(container_last_seen[30d])` ohne Pod-Filter liefert lastSeen fuer alle Pods in einer Query. ~55 Calls -> 1 Call.

**Implementierungsvorschlag:** Neue Methoden im `PrometheusClient` Interface:

```java
/** Holt TimeRanges fuer ALLE Pods in einer Batch-Query. */
Map<String, TimeRange> fetchAllPodTimeRanges(String podLabel);

/** Holt lastSeen fuer ALLE Pods in einer Batch-Query. */
Map<String, ZonedDateTime> fetchAllPodLastSeen(String podLabel);

/** Holt TimeRanges fuer ALLE Server in einer Batch-Query. */
Map<String, TimeRange> fetchAllServerTimeRanges();
```

---

### A2. PrometheusTimeSeriesSource: Batch-Abfrage fuer Knoten-Listen

**Betroffene Procedures (indirekt, via `TimeSeriesUtil`):**
- `graphobs.aggregation.aggregate_nodes` — nimmt `List<Node>`, ruft pro Node `getTimeSeries()` auf
- `graphobs.data.get_all_for_service` — findet alle Pods eines Service, ruft pro Pod `getTimeSeries()` auf
- `graphobs.comparison.compare_ts_mean` — nimmt `List<Node>`, holt pro Node eine Zeitreihe
- `graphobs.comparison.compare_periods_means` — nimmt `List<Node>`, holt pro Node zwei Zeitreihen (2 Perioden)
- `graphobs.comparison.compare_two_groups` — nimmt zwei `List<Node>`, holt pro Node eine Zeitreihe
- `graphobs.analysis.node_group_correlation` — nimmt `List<Node>` nodesB, holt pro Node eine Zeitreihe
- `graphobs.analysis.var_irf_node_group` — nimmt `List<Node>` nodesB, holt pro Node eine Zeitreihe

**Problem:** Wenn 20 Pods uebergeben werden, macht `PrometheusTimeSeriesSource` 20 separate HTTP-Requests an `/api/v1/query_range`.

**Loesung:** Prometheus unterstuetzt Label-Regex fuer Batch-Abfragen:

```promql
-- Statt 20x einzeln:
container_cpu_usage_seconds_total{pod="pod-1"}
container_cpu_usage_seconds_total{pod="pod-2"}
...

-- Einmal mit Regex:
container_cpu_usage_seconds_total{pod=~"pod-1|pod-2|pod-3|..."}
```

**Implementierungsvorschlag:** Neue Methode in `PrometheusTimeSeriesSource`:

```java
/** Holt Zeitreihen fuer mehrere Pods/Knoten in einer Prometheus-Query. */
Map<String, TimeSeries> getTimeSeriesBatch(List<Node> nodes, String metric, TimeWindow window);
```

`TimeSeriesUtil` bekommt eine `getFilteredTimeSeriesBatch()` Variante, die von den `List<Node>` Procedures genutzt wird.

---

### A3. Jaeger: Batch-Abfrage fuer mehrere Services

**Betroffene Procedures:**
- `graphobs.data.get_traces_for_subgraph` — extrahiert Services aus Operations, fragt Jaeger **pro Service** separat ab

**Problem:** Wenn ein Subgraph 5 Services umfasst, werden 5 separate Jaeger-Requests gemacht.

**Loesung:** Begrenzt, da die Jaeger API keinen Multi-Service-Endpunkt hat. Aber die Requests koennten **parallel** (via `CompletableFuture`) statt sequenziell laufen.

---

### A4. `Thread.sleep(10_000)` in PrometheusRegistrar entfernen

**Betroffene Klasse:** `PrometheusRegistrar.registerPrometheus()`

**Problem:** 10 Sekunden harter Sleep nach jeder Registrierung, bevor ServicePodLinker laeuft.

**Loesung:** Sleep durch Event-basierte Logik ersetzen, oder den Linker direkt nach dem Bulk-Write ausfuehren (die Daten sind zu dem Zeitpunkt bereits committed).

---

## Teil B: Fehlende Batch-/Listen-Varianten von Procedures

Mehrere Procedures akzeptieren nur einen einzelnen Knoten, obwohl der typische Use-Case eine Liste ist (z.B. "Logs fuer alle Pods eines Service"). In Cypher muss man dann `UNWIND` + Subquery nutzen, was N sequenzielle Procedure-Aufrufe erzeugt.

### B1. `get_logs_for` — Listen-Variante

**Aktuell:** `graphobs.data.get_logs_for(node, index, params)` — ein Node

**Fehlend:** `graphobs.data.get_logs_for_nodes(nodes, index, params)` — Liste von Nodes

**Use-Case:**
```cypher
-- Aktuell: N sequenzielle OpenSearch-Requests
MATCH (s:Service {id: "checkout"})-[:CALLS]->(dep:Service)
CALL graphobs.data.get_logs_for(dep, "otel-logs-*", {level: "error"})
YIELD timestamp, message, source
RETURN source, count(*) AS errors

-- Besser: 1 OpenSearch-Request mit OR-Filter
MATCH (s:Service {id: "checkout"})-[:CALLS]->(dep:Service)
WITH collect(dep) AS deps
CALL graphobs.data.get_logs_for_nodes(deps, "otel-logs-*", {level: "error"})
YIELD timestamp, message, source
RETURN source, count(*) AS errors
```

**Implementierung:** Baut eine `bool.should`-Query mit allen Service-Namen aus der Knotenliste. Ein einziger OpenSearch-Request statt N.

---

### B2. `get_spans` — Knoten-basierte Variante(n)

**Aktuell:** `graphobs.data.get_spans(serviceName, params)` — nimmt Service als String

**Fehlend:**
- `graphobs.data.get_spans_for(node, params)` — nimmt Service/Pod/Operation-Node, loest Service-Name automatisch auf
- `graphobs.data.get_spans_for_nodes(nodes, params)` — Liste von Nodes, baut Batch-Query

**Use-Case:**
```cypher
-- Aktuell muss man den Namen manuell extrahieren
MATCH (s:Service {id: "frontend"})-[:CALLS]->(dep:Service)
CALL graphobs.data.get_spans(dep.id, {range: "-1h"})
...

-- Besser: Knoten direkt uebergeben
MATCH (s:Service {id: "frontend"})-[:CALLS]->(dep:Service)
CALL graphobs.data.get_spans_for(dep, {range: "-1h"})
...
```

---

### B3. `get_trace` — Listen-Variante

**Aktuell:** `graphobs.data.get_trace(serviceName, params)` — ein Service

**Fehlend:** `graphobs.data.get_traces_for_services(serviceNames, params)` oder Knoten-basiert

**Use-Case:** "Zeige mir Traces fuer alle Services in meiner Call-Chain"

---

### B4. `get_time_series` — Listen-Variante

**Aktuell:** `graphobs.data.get_time_series(node, tsName, params)` — ein Node

**Fehlend:** `graphobs.data.get_time_series_batch(nodes, tsName, params)` — Liste von Nodes

**Use-Case:** Zeitreihen fuer alle Pods eines Services in einer Abfrage holen (intern eine Prometheus-Query mit `pod=~"pod-1|pod-2|..."` statt N Queries).

**Hinweis:** `graphobs.aggregation.aggregate_nodes` macht intern bereits etwas Aehnliches, gibt aber nur das Aggregat zurueck, nicht die Einzel-Zeitreihen.

---

### B5. `get_statistic_from_node` — Listen-Variante

**Aktuell:** `graphobs.data.get_statistic_from_node(node, tsName, params)` — ein Node

**Fehlend:** `graphobs.data.get_statistics_batch(nodes, tsName, params)` — Liste von Nodes

**Use-Case:** Statistiken (mean, p99, etc.) fuer alle Pods eines Service in einer Abfrage.

---

### B6. `get_owning_service` — Listen-Variante

**Aktuell:** `graphobs.search.get_owning_service(node)` — ein Node

**Fehlend:** `graphobs.search.get_owning_services(nodes)` — Liste von Nodes

**Use-Case:** Fuer eine Liste von Pods die zugehoerigen Services finden.

---

## Teil C: Vorschlaege zur Projektstruktur

### C1. Deprecated-Code aufraumen

Folgende Dateien sind deprecated/auskommentiert und koennten entfernt werden:

| Datei | Grund |
|---|---|
| `prometheus_registration_old/` (ganzes Package) | Ersetzt durch `prometheus_registration_II` |
| `PrometheusTimeSeriesSource_old.java` | Ersetzt durch `PrometheusTimeSeriesSource.java` |
| `GetTimeSeries_old.java` | Ersetzt durch `GetTimeSeries.java` |
| `ServiceAggregation_old.java` | Ersetzt durch `ServiceAggregation.java` |
| `JaegerTraceToSubgraphProcedure.java` | Ersetzt durch `TraceToSubgraphProcedure_new.java` |
| `PointCut_by_Values_old.java` | Ersetzt durch `PointCut_by_Values.java` |

Ausserdem: Viele Procedures in `aggregation_functions/` sind auskommentiert (`/*@Procedure...*/`) mit `decrapted.*` Namespace. Diese toten Code-Bloecke koennten entfernt werden.

### C2. Inkonsistente Procedure-Namespaces vereinheitlichen

Aktuell gibt es drei Namespaces:

| Namespace | Anzahl | Status |
|---|---|---|
| `graphobs.*` | ~40+ | Aktuell/Aktiv |
| `timegraph.*` | ~5 | Alt, teilweise noch aktiv |
| `timeGraph.*` | ~5 | Alt, teilweise noch aktiv |
| `decrapted.*` | ~5 | Deprecated |
| `myplugin.*` | 1 | Test-Procedure |

Vorschlag: Alle aktiven auf `graphobs.*` migrieren, alte entfernen.

### C3. Package-Struktur

Aktuelle Probleme:
- Top-Level-Packages (`aggregation_functions/`, `comparison/`, `time_relations/`, etc.) haben keine gemeinsame Hierarchie
- `util/trace/sources/` enthaelt sowohl Jaeger- als auch OpenSearch-Procedures
- `prometheus_registration_II` hat `_II` im Namen (Ueberbleibsel vom Refactoring)

Moegliche Reorganisation (optional, Aufwand vs. Nutzen abwaegen):

```
graphobs/
├── core/                    # Plugin-Lifecycle
├── matching/                # Entity-Matching
├── datasources/
│   ├── prometheus/          # Registrierung + HTTP-Client
│   ├── jaeger/              # Import + HTTP-Client
│   └── opensearch/          # Registrierung + HTTP-Client
├── query/
│   ├── timeseries/          # get_time_series, get_statistic, aggregations
│   ├── traces/              # get_trace, get_spans, subgraph-queries
│   ├── logs/                # get_logs, get_logs_for
│   └── temporal/            # time_relations, time_cuts
├── analysis/
│   ├── comparison/          # t-Tests, Z-Tests
│   ├── correlation/         # Pearson, Kendall, VAR
│   └── detection/           # Changepoints, Outliers
└── search/                  # Call-Chains, Ownership
```

### C4. Shared HTTP-Client

Aktuell nutzen verschiedene Module unterschiedliche HTTP-Mechanismen:
- `PrometheusHttpClient`: `HttpURLConnection` (alt)
- `OpenSearchHttpClient`: `java.net.http.HttpClient` (modern)
- Jaeger-Procedures: `java.net.http.HttpClient` (modern)

Vorschlag: Alle auf `java.net.http.HttpClient` vereinheitlichen. Optional: Shared Client-Instanz (Connection Pooling).

---

## Priorisierung

| Optimierung | Impact | Aufwand | Prioritaet |
|---|---|---|---|
| A1: Batch-PromQL fuer Registrierung | Sehr hoch (40s -> 3s) | Mittel | **1** |
| A4: Thread.sleep entfernen | Mittel (10s gespart) | Trivial | **2** |
| A2: Batch Prometheus fuer List-Procedures | Hoch (skaliert mit Knotenanzahl) | Mittel | **3** |
| B1: get_logs_for_nodes | Mittel | Klein | **4** |
| B2: get_spans_for (Node-basiert) | Mittel | Klein | **5** |
| C1: Deprecated-Code aufraumen | Wartbarkeit | Klein | **6** |
| C2: Namespaces vereinheitlichen | Konsistenz | Klein | **7** |
| B4: get_time_series_batch | Hoch bei vielen Nodes | Mittel | **8** |
| A3: Jaeger parallel | Mittel | Klein | **9** |
| C3: Package-Reorganisation | Wartbarkeit | Hoch | **10** |
| C4: Shared HTTP-Client | Sauberkeit | Mittel | **11** |
