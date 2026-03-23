# Projekt-Uebersicht: GraphObs Neo4j Plugin

## Zweck

Neo4j-Plugin fuer Graph-basierte Observability. Integriert drei Datenquellen (Prometheus, Jaeger, OpenSearch) in einen einheitlichen Property-Graph und stellt Cypher-Procedures fuer Zeitreihen-Analyse, Trace-Abfragen, Log-Suche und statistische Vergleiche bereit.

---

## Package-Struktur

```
src/main/java/
├── core/                              # Plugin-Lifecycle, Auto-Registrierung
├── util/
│   ├── matching/                      # Cross-Source Entity-Matching
│   ├── sources/                       # Datenquellen-Adapter (Local, Prometheus)
│   ├── source_registration/
│   │   ├── prometheus_registration_II/  # Prometheus-Anbindung (aktiv)
│   │   ├── prometheus_registration_old/ # Alte Prometheus-Version (deprecated)
│   │   └── opensearch_registration/     # OpenSearch-Anbindung
│   └── trace/
│       ├── datatype/datastructs/      # Jaeger DTOs (Span, Trace, etc.)
│       ├── datatype/graph/            # Graph-DTOs (GraphNode, GraphEdge)
│       ├── source_registration/       # Jaeger Graph-Import
│       └── sources/                   # Jaeger/OpenSearch Query-Procedures
├── ts_querys/                         # Zeitreihen-Abfrage und Statistik
├── aggregation_functions/             # Zeitreihen-Aggregationen
├── topological_aggregation/           # Service/Knoten-uebergreifende Aggregation
├── comparison/                        # Statistische Vergleiche (t-Tests, Z-Tests)
├── mathematical_relations/            # Korrelation, VAR, Impact-Analyse
├── time_relations/                    # Temporale Suche (before, after, within, ...)
├── time_cuts/                         # Zeitreihen-Schnitte (Point/Period)
├── time_series_analysis/              # Changepoint-Detection, Outlier-Detection
├── structural_search/                 # Graph-Traversierung (Call-Chains, Ownership)
├── temporal_joins/                    # Zeitreihen-Alignment (Interpolation, Forward-Fill)
└── result_classes/                    # Gemeinsame Return-Types
```

---

## Module im Detail

### 1. Core (`core/`)

| Datei | Beschreibung |
|---|---|
| `MyNeo4jPluginExtensionFactory` | Plugin-Einstiegspunkt. Erstellt Indexes, registriert Datenquellen via Env-Vars (`PROMETHEUS_URL`, `JAEGER_URL`, `OPENSEARCH_URL`) bei Neo4j-Start. |
| `MyTransactionEventListener` | Validiert/normalisiert `start`/`end` Properties auf `time_series` Knoten bei jedem Commit. |

### 2. Matching (`util/matching/`)

| Datei | Beschreibung |
|---|---|
| `FieldNameResolver` | Zentrale Feld-Aufloesung ueber Kandidatenlisten. Unterstuetzt verschachtelte + gemischte Key-Formate. Bildet Graph-Knoten auf OpenSearch-Filter ab. |
| `OperationIdParser` | Kanonisches Format `"service::operation"` fuer Operation-IDs. |
| `EntityNameNormalizer` | Normalisiert Pod/Service/Server-Namen (z.B. Namespace-Prefix entfernen). |
| `ServicePodMatcher` | Verlinkt Service- und Pod-Knoten via Longest-Match auf Namen. |

### 3. Datenquellen-Adapter (`util/sources/`)

| Datei | Beschreibung |
|---|---|
| `TimeSeriesSource` | Interface fuer Zeitreihen-Quellen (Methode: `getTimeSeries()`). |
| `LocalTimeSeriesSource` | Liest Zeitreihen aus lokalen `time_series` Knoten im Graph. |
| `PrometheusTimeSeriesSource` | Holt Zeitreihen live via Prometheus HTTP API (`/api/v1/query_range`). Unterstuetzt `rate()`, `histogram_quantile()`, Raw-Metriken. |

### 4. Prometheus-Registrierung (`util/source_registration/prometheus_registration_II/`)

| Datei | Beschreibung |
|---|---|
| `PrometheusClient` | Interface: fetchMetricNames, fetchPods, getPodTimeRange, etc. |
| `PrometheusHttpClient` | HTTP-Implementierung mit Jackson JSON, URL-Encoding, ZonedDateTime-Konvertierung. |
| `PodRepository` / `Neo4jPodRepository` | CRUD fuer Pod/Server-Knoten im Graph. Bulk-Upsert via UNWIND. |
| `PrometheusRegistrationService` | Orchestriert Erstregistrierung: Pods + Server entdecken und mit TimeRanges in Graph schreiben. |
| `PrometheusToNeo4jUpdater` | Periodischer Background-Sync (Singleton pro URL). Erkennt neue/gestoppte Pods/Server. |
| `PrometheusRegistrar` | Procedure `graphobs.datasources.register_prometheus`. |
| `ServicePodLinker` | Verlinkt Services mit Pods nach Registrierung (delegiert an `ServicePodMatcher`). |

### 5. OpenSearch-Registrierung (`util/source_registration/opensearch_registration/`)

| Datei | Beschreibung |
|---|---|
| `OpenSearchClient` / `OpenSearchHttpClient` | HTTP-Client mit optionaler Basic Auth, POST-basierter Suche. |
| `LogRepository` / `Neo4jLogRepository` | CRUD fuer `(:OpenSearch)` und `(:LogIndex)` Knoten. |
| `OpenSearchRegistrationService` | Ping, Indices auflisten, Metadaten speichern. |
| `OpenSearchToNeo4jUpdater` | Periodischer Index-Sync (analog Prometheus-Updater). |
| `OpenSearchRegistrar` | Procedure `graphobs.datasources.register_opensearch`. |

### 6. Jaeger-Integration (`util/trace/`)

| Datei | Beschreibung |
|---|---|
| `JaegerImportProcedure` | Procedure `graphobs.jaeger.import_graph`. Importiert Service/Operation/Dependency-Graph aus Jaeger. |
| `JaegerGetTraceProcedure` | Procedure `graphobs.data.get_trace`. Holt Traces von Jaeger mit Filtern. |
| `JaegerGetSpansProcedure` | Procedure `graphobs.data.get_spans`. Holt einzelne Spans mit Tag-Filtern. |
| `JaegerDependencyProcedure` | Procedure `graphobs.jaeger.call_count_between`. Call-Count zwischen zwei Services. |
| `JaegerTraceToSubgraphProcedure` | Procedure `timegraph.data.cypher_for_trace_subgraph_visualize_old`. Baut Cypher aus Trace (alte Version). |
| `TraceToSubgraphProcedure_new` | Procedure `graphobs.data.cypher_for_trace_subgraph_visualize`. Parametrisierte Version. |
| `JaegerSubgraphToTraceProcedure` | Procedure `graphobs.data.get_traces_for_subgraph`. Findet Traces die einen Subgraph beruehren. |
| `OpenSearchGetLogsProcedure` | Procedures `graphobs.data.get_logs` + `graphobs.data.get_logs_for`. |

### 7. Zeitreihen-Abfrage (`ts_querys/`)

| Datei | Beschreibung |
|---|---|
| `GetTimeSeries` | Procedures `graphobs.data.get_time_series` (Node) und `graphobs.data.get_time_series_from_pod` (String). Zentrale Zeitreihen-Abfrage mit optionaler Aggregation. |
| `GetStatistic` | Procedures `graphobs.data.get_statistic_from_node` (Node) und `graphobs.data.get_statistic_from_pod` (String). Statistiken (min, max, mean, median, stddev, etc.). |

### 8. Aggregation (`aggregation_functions/`)

| Procedure | Beschreibung |
|---|---|
| `graphobs.aggregation.binned_average` | Gleitender Durchschnitt in regelmaessigen Zeitintervallen |
| `graphobs.aggregation.cumulative_sum` | Kumulierte Summe |
| `graphobs.aggregation.integral` | Integral (Wert x Zeitabstand) |
| `graphobs.aggregation.difference` | Differenz aufeinanderfolgender Werte |
| `graphobs.aggregation.derivative` | Aenderungsrate pro Sekunde |
| `graphobs.aggregation.linear_regression` | Lineare Regression |
| `graphobs.aggregation.moving_average` | Gleitender Durchschnitt nach Fenstergroesse |

### 9. Topologische Aggregation (`topological_aggregation/`)

| Procedure | Beschreibung |
|---|---|
| `graphobs.data.get_all_for_service` | Aggregiert Metrik ueber alle Pods eines Services (Node -> Pods -> Zeitreihen). |
| `graphobs.aggregation.aggregate_nodes` | Aggregiert Metrik ueber beliebige Knotenliste. |

### 10. Statistische Vergleiche (`comparison/`)

| Procedure | Beschreibung |
|---|---|
| `graphobs.comparison.compare_ts_mean` | Vergleicht Mittelwerte einer Metrik ueber zwei Gruppen (t-Test). |
| `graphobs.comparison.compare_periods_means` | Vergleicht Mittelwerte zweier Zeitraeume pro Knoten (t-Test). |
| `graphobs.comparison.compare_periods_paired` | Gepaarter t-Test fuer eine Zeitreihe ueber zwei Perioden. |
| `graphobs.comparison.compare_two_groups` | Vergleicht zwei explizite Knotengruppen (t-Test). |
| `graphobs.comparison.compare_proportions` | Proportions-Vergleich (Z-Test) zwischen zwei Knoten. |

### 11. Mathematische Relationen (`mathematical_relations/`)

| Procedure | Beschreibung |
|---|---|
| `graphobs.analysis.correlation` | Pearson/Kendall-Korrelation zwischen zwei Zeitreihen (Roh-Daten). |
| `graphobs.analysis.node_group_correlation` | Korrelation eines Knotens gegen eine Knotengruppe. |
| `graphobs.analysis.var_irf` | VAR(1)-Modell mit Impuls-Antwort-Funktion (Roh-Daten). |
| `graphobs.analysis.var_irf_node_group` | VAR(1) IRF eines Knotens gegen Knotengruppe. |
| `graphobs.analysis.quantify_event_impact` | Peak Impact und Impact Duration nach einem Event. |
| `graphobs.analysis.correlate_events_with_value_by_id` | Korrelation zwischen Event-Haeufigkeit und Zeitreihen-Wert. |

### 12. Zeitreihen-Analyse (`time_series_analysis/`)

| Procedure | Beschreibung |
|---|---|
| `graphobs.analysis.pelt` | PELT-Algorithmus fuer Changepoint-Detection. |
| `graphobs.analysis.detect_regression_outliers` | Outlier-Detection via lokaler linearer Regression. |

### 13. Temporale Suche (`time_relations/`)

Alle arbeiten rein auf dem Graph (keine externen Calls). Jeweils `_by_object` (Referenz-Knoten) und `_by_value` (ISO-String) Varianten:

| Procedure-Paar | Beschreibung |
|---|---|
| `graphobs.time_search.before_*` | Knoten die vollstaendig VOR dem Zeitpunkt liegen |
| `graphobs.time_search.after_*` | Knoten die vollstaendig NACH dem Zeitpunkt liegen |
| `graphobs.time_search.within_*` | Knoten deren Zeitraum vollstaendig INNERHALB liegt |
| `graphobs.time_search.outside_*` | Knoten deren Zeitraum vollstaendig AUSSERHALB liegt |
| `graphobs.time_search.overlap_*` | Knoten mit Ueberlappung |
| `graphobs.time_search.start_in_*` | Knoten deren Start im Zeitraum liegt |
| `graphobs.time_search.end_in_*` | Knoten deren Ende im Zeitraum liegt |
| `graphobs.time_search.next_*` | Naechstfolgender Knoten nach Zeitpunkt |
| `graphobs.time_search.last_*` | Vorheriger Knoten vor Zeitpunkt |
| `graphobs.time_search.range_*` | Knoten in +/- Bereich um Zeitpunkt |

### 14. Strukturelle Suche (`structural_search/`)

| Procedure | Beschreibung |
|---|---|
| `graphobs.search.get_calling_services` | Services die einen Service aufrufen (rueckwaerts) |
| `graphobs.search.get_called_services` | Services die ein Service aufruft (vorwaerts) |
| `graphobs.search.get_calling_services_from_operation` | Services die eine Operation aufrufen |
| `graphobs.search.get_called_services_from_operation` | Services die von einer Operation aufgerufen werden |
| `graphobs.search.get_calling_operations_from_operation` | Operationen die eine Operation aufrufen |
| `graphobs.search.get_called_operations_from_operation` | Operationen die von einer Operation aufgerufen werden |
| `graphobs.search.get_calling_operations_from_service` | Operationen die irgendeinen Service-Endpunkt aufrufen |
| `graphobs.search.get_called_operations_from_service` | Operationen die von einem Service aufgerufen werden |
| `graphobs.search.get_owning_service` | Findet den besitzenden Service eines Knotens |
| `graphobs.search.get_owning_service_by_name` | Findet den besitzenden Service via Name |

### 15. Zeitreihen-Schnitte (`time_cuts/`)

| Procedure | Beschreibung |
|---|---|
| `timeGraph.time.time_series.PointCut_by_Values` | Schneidet Zeitreihe an einem Zeitpunkt |
| `timeGraph.time.time_series.PointCut_by_Object` | Schneidet Zeitreihe an einem Zeit-Knoten |
| `timeGraph.time.time_series.PeriodCut_by_Values` | Schneidet Zeitreihe auf Zeitraum (ISO-Strings) |
| `timeGraph.time.time_series.PeriodCut_by_Object` | Schneidet Zeitreihe auf Zeitraum eines Knotens |

### 16. Temporal Joins (`temporal_joins/`)

Hilfsklassen (keine Procedures):

| Datei | Beschreibung |
|---|---|
| `TemporalJoinStrategy` | Interface fuer Alignment-Strategien |
| `LinearInterpolationJoinStrategy` | Lineare Interpolation zwischen Zeitpunkten |
| `ForwardFillJoinStrategy` | Letzter bekannter Wert wird fortgeschrieben |
| `ResamplingJoinStrategy` | Resampling auf regelmaessiges Gitter |
| `JoinStrategyFactory` | Factory fuer Strategy-Auswahl |
| `TimeSeriesConverter` | Konvertiert zwischen internen Formaten |
| `AlignedData` | Ergebnis-Container fuer aligned Data |

### 17. Utility (`util/`)

| Datei | Beschreibung |
|---|---|
| `TimeSeriesUtil` | Zentrale Zeitreihen-Utility. Routet zwischen Local und Prometheus, parsiert Zeit-Parameter, wendet Aggregationen an. |
| `AggregationUtil` | Wendet Aggregationsfunktionen (binned_average, difference, etc.) an. |
| `StatisticUtil` | Berechnet min, max, mean, median, stddev, sum, count, percentile. |
| `ComparisonUtil` | Helper fuer Vergleichs-Procedures: berechnet Mittelwerte pro Knoten/Periode. |
| `TimeWindow` | Immutable Zeitfenster (startTime, endTime). |

---

## Graph-Schema

```
(:Prometheus {url, names[]})
(:Pod:time_period {name, start, end, status}) -[:HAS_TIME_SERIES]-> (:Prometheus)
(:Server:time_period {name, start, end, status}) -[:HAS_TIME_SERIES]-> (:Prometheus)
(:Pod) -[:DEPLOYED_ON]-> (:Server)
(:Service) -[:HAS_POD]-> (:Pod)
(:Service) -[:HAS_TIME_SERIES]-> (:Prometheus)
(:Operation) -[:HAS_TIME_SERIES]-> (:Prometheus)

(:Jaeger {baseUrl})
(:Service {id, name}) -[:HAS_OPERATION]-> (:Operation {id})
(:Operation) -[:DEPENDS_ON]-> (:Operation)

(:OpenSearch {url, indexPattern, user, password})
(:LogIndex {name, docCount, fieldCount}) -[:STORED_IN]-> (:OpenSearch)

(:time_series {start, end, <metric-values>[]}) -[:HAS_TIME_SERIES]-> (:Pod|:Service|:Operation)
```

---

## Externe Abhaengigkeiten (Datenfluss)

```
Prometheus HTTP API  <---  PrometheusTimeSeriesSource (fuer Zeitreihen-Abfragen)
                     <---  PrometheusHttpClient (fuer Registrierung/Updates)

Jaeger HTTP API      <---  JaegerGetTraceProcedure, JaegerGetSpansProcedure,
                           JaegerDependencyProcedure, JaegerSubgraphToTraceProcedure,
                           JaegerImportProcedure

OpenSearch HTTP API  <---  OpenSearchGetLogsProcedure (get_logs, get_logs_for)
                     <---  OpenSearchHttpClient (fuer Registrierung/Updates)
```
