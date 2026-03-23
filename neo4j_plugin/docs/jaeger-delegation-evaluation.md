# Bewertung: Jaeger-Procedures im Plugin vs. Delegation an Jaeger

## Ueberblick

Das Plugin enthaelt aktuell **8 Jaeger-bezogene Procedures**, die Trace-, Span- und Abhaengigkeitsdaten
aus Jaeger holen und verarbeiten. Dieses Dokument bewertet jede Procedure hinsichtlich:

- **Aktuelle Logik:** Was macht die Procedure? Welche Verarbeitung findet im Plugin statt?
- **Jaeger-API-Faehigkeiten:** Kann Jaeger die Verarbeitung serverseitig uebernehmen?
- **Performance-Gewinn:** Wie gross waere der Vorteil bei Delegation?
- **Erweiterbarkeitsrisiko:** Was geht verloren, wenn die Logik in Jaeger wandert?
- **Empfehlung:** Plugin behalten / An Jaeger delegieren / Hybrid

### Bewertungsskala

| Symbol | Bedeutung |
|--------|-----------|
| ++ | Sehr hoher Performance-Gewinn bei Delegation |
| + | Moderater Performance-Gewinn |
| o | Kein wesentlicher Unterschied |
| - | Erweiterbarkeit eingeschraenkt |
| -- | Delegation nicht moeglich/sinnvoll |

### Kontext: Jaeger API vs. Prometheus API

Im Gegensatz zu Prometheus, das eine maechtige Abfragesprache (PromQL) mit serverseitiger Aggregation
bietet, ist die **Jaeger Query API relativ simpel**. Sie unterstuetzt:

- Filtern nach Service, Operation, Tags, Zeitfenster, Limit
- Abrufen einzelner Traces nach TraceID
- Abrufen von Service-Abhaengigkeiten (Call Counts)

Sie bietet **keine** serverseitige Aggregation, Statistikberechnung oder komplexe Filterung
(z.B. "alle Traces die ALLE dieser Operations enthalten"). Dadurch ist das Delegationspotential
grundsaetzlich geringer als bei Prometheus.

---

## 1. Trace-Abruf

### 1.1 `graphobs.data.get_trace` (JaegerGetTraceProcedure)

**Aktuelle Logik:**
- HTTP-Request an Jaeger `/traces?service=...&start=...&end=...&limit=...`
- Client-seitiges Filtern nach Operation (Substring-Match) und HTTP Status Code
- Berechnung von Trace-Metriken: spanCount, startTime (min), durationMs (max end - min start)
- Rueckgabe als TraceResult mit traceJSON

**Plugin-seitige Verarbeitung:**
1. Operation-Filter: Exact + Substring Match ueber alle Spans
2. Status-Code-Filter: Prueft `http.status_code`, `status.code`, `status` Tags
3. Trace-Metriken: Berechnung aus Span-Timestamps

**Delegationspotential:**

| Aspekt | Bewertung |
|--------|-----------|
| Operation-Filter | Teilweise delegierbar — Jaeger API unterstuetzt `operation=` Parameter, aber nur Exact Match. Plugin macht zusaetzlich Substring-Match. |
| Status-Code-Filter | Teilweise delegierbar — Jaeger API unterstuetzt `tag=status_code:200`, aber nur wenn der Tag exakt so heisst. Plugin probiert 3 verschiedene Tag-Keys. |
| Trace-Metriken | Nicht delegierbar — Jaeger liefert keine aggregierten Metriken pro Trace. |

**Performance:** o — Die Datenmenge ist durch `limit` begrenzt (default 1000). Die Hauptlast ist der
HTTP-Transport, nicht die Client-Verarbeitung. Das Senden des `operation`-Parameters an Jaeger
geschieht bereits.

**Erweiterbarkeit:** Die Client-seitige Verarbeitung erlaubt:
- Flexible Tag-Key-Aufloesung (3 Varianten fuer Status Code)
- Substring-Match fuer Operations (nuetzlich fuer partielle Namen)
- Einfache Erweiterung um weitere Filter-Kriterien
- Unabhaengigkeit von Jaeger-Versionen (unterschiedliche Tag-Namenskonventionen)

**Empfehlung: Im Plugin belassen** — Minimales Delegationspotential, da Jaeger API bereits den
Grossteil der Filterung uebernimmt. Client-seitige Flexibilitaet fuer Tag-Varianten ist wertvoll.

---

### 1.2 `graphobs.data.get_traces_for_services` (neu, B3)

**Aktuelle Logik:**
- Iteriert ueber Liste von Service-Namen
- Ruft `get_trace()` pro Service auf
- Dedupliziert Traces nach traceId

**Delegationspotential:**

| Aspekt | Bewertung |
|--------|-----------|
| Multi-Service-Query | Nicht delegierbar — Jaeger API hat keinen Multi-Service-Endpunkt |
| Parallelisierung | Verbesserbar — Requests koennten parallel via CompletableFuture laufen |

**Performance:** + — Bei vielen Services (z.B. 10) koennten parallele Requests die Latenz um
Faktor 5-10 reduzieren. Aber: nicht durch Delegation an Jaeger loesbar, nur durch Parallelisierung
im Plugin.

**Empfehlung: Im Plugin belassen, Parallelisierung erwaegen (A3)**

---

### 1.3 `graphobs.data.get_traces_for_subgraph` (JaegerSubgraphToTraceProcedure)

**Aktuelle Logik:**
- Extrahiert Operation-IDs aus uebergebenen Nodes oder String-Liste
- Leitet Service-Namen aus Operations ab (via OperationIdParser)
- Query pro Service an Jaeger
- **Komplexe Client-seitige Filterung:** Fuer jeden Trace wird geprueft, ob er ALLE angegebenen
  Operations enthaelt (nicht nur eine!)
- Deduplizierung ueber seenTraceIds

**Plugin-seitige Verarbeitung:**
1. Operation-zu-Service-Mapping ueber Graph-Knoten und OperationIdParser
2. Process-Tag-Aufloesung: `processes`-Map → spanId → service::operation
3. Subgraph-Matching: `traceOps.containsAll(operationIds)` — der Trace muss ALLE angeforderten
   Operations enthalten

**Delegationspotential:**

| Aspekt | Bewertung |
|--------|-----------|
| ALL-Match Filterung | Nicht delegierbar — Jaeger kann nicht "Traces die Operations A UND B UND C enthalten" filtern |
| Multi-Service Query | Nicht delegierbar — kein Multi-Service-Endpunkt |
| Operation→Service Mapping | Nicht delegierbar — nutzt Plugin-eigene OperationIdParser-Konvention |

**Performance:** -- — Die Kernlogik (ALL-Match ueber Operations) ist in Jaeger nicht ausdrueckbar.
Selbst mit einer hypothetischen erweiterten Jaeger-API waere die `containsAll`-Semantik komplex.

**Erweiterbarkeit:**
- Die Subgraph-Matching-Logik ist eine **einzigartige Plugin-Faehigkeit**, die Jaeger nicht bietet
- Sie verbindet Graph-Topologie (Neo4j) mit Trace-Daten (Jaeger) — genau die Art von
  Cross-Source-Analyse, fuer die das Plugin existiert
- Zukuenftige Erweiterungen koennten: Gewichtetes Matching, partielle Matches, temporale Korrelation

**Empfehlung: Im Plugin belassen** — Kern-Feature des Plugins, nicht delegierbar.

---

## 2. Span-Abruf

### 2.1 `graphobs.data.get_spans` (JaegerGetSpansProcedure)

**Aktuelle Logik:**
- HTTP-Request an Jaeger `/traces?service=...&operation=...&tag=...&start=...&end=...&limit=...`
- Jaeger filtert serverseitig nach Service, Operation, Tags
- Plugin extrahiert einzelne Spans aus den Traces
- Merge von Process-Tags (Pod-Name etc.) mit Span-Tags
- Zusaetzliches Client-seitiges Tag-Matching als Sicherheits-Fallback

**Plugin-seitige Verarbeitung:**
1. Span-Extraktion aus Trace-Struktur (Jaeger liefert Traces, nicht einzelne Spans)
2. Process-Tag-Merge: Pod-Name aus `processes`-Map in Span-Tags einfliessen lassen
3. Custom Tag Filter Fallback: Prueft Tags die Jaeger moeglicherweise nicht serverseitig filtern konnte

**Delegationspotential:**

| Aspekt | Bewertung |
|--------|-----------|
| Tag-Filter | Bereits delegiert — `&tag=key:value` wird an Jaeger geschickt |
| Operation-Filter | Bereits delegiert — `&operation=` wird an Jaeger geschickt |
| Span-Extraktion | Nicht delegierbar — Jaeger API liefert keine isolierten Spans |
| Process-Tag-Merge | Nicht delegierbar — Jaeger liefert Process-Tags separat von Span-Tags |

**Performance:** o — Die meiste Filterung passiert bereits serverseitig. Der Client-Overhead
(Span-Extraktion, Tag-Merge) ist minimal und proportional zur Ergebnisgrösse.

**Erweiterbarkeit:**
- Process-Tag-Merge ist essentiell fuer Pod-Level-Filterung (Kubernetes)
- Fallback-Tag-Matching faengt Inkonsistenzen in Jaeger-Versionen ab
- Andere Tracing-Backends (Zipkin, Tempo) koennten andere Span-Formate liefern

**Empfehlung: Im Plugin belassen** — Bereits optimal delegiert. Restliche Logik ist nicht delegierbar.

---

### 2.2 `graphobs.data.get_spans_for` / `get_spans_for_nodes` (neu, B2)

**Aktuelle Logik:**
- Loest Graph-Knoten (Service/Pod/Operation) zu Service-Namen auf
- Delegiert an `get_spans()` — gleiche Jaeger-Interaktion

**Plugin-seitige Verarbeitung:**
1. Node-zu-Service-Aufloesung ueber Graph-Labels und OperationIdParser
2. Fuer Pods: Graph-Traversal zu owning Service
3. Bei mehreren Nodes: Deduplizierung der Service-Namen

**Delegationspotential:** Nicht delegierbar — die Node-zu-Service-Aufloesung nutzt Neo4j-Graph-Daten,
die Jaeger nicht kennt.

**Empfehlung: Im Plugin belassen** — Graph-zu-Tracing-Bridge, Kernkompetenz des Plugins.

---

## 3. Trace-Visualisierung

### 3.1 `graphobs.data.cypher_for_trace_subgraph_visualize` (TraceToSubgraphProcedure_new)

**Aktuelle Logik:**
- Holt einzelnen Trace per TraceID von Jaeger
- Extrahiert Services, Operations und DEPENDS_ON-Beziehungen aus Spans
- Baut parameterisierte Cypher-Query: `MATCH (s:Service) WHERE s.id IN $svcIds ...`
- Fuehrt Cypher gegen Neo4j aus und liefert Nodes + Relationships fuer Visualisierung

**Plugin-seitige Verarbeitung:**
1. Span-Analyse: Process-Aufloesung, OperationId-Aufbau
2. Dependency-Erkennung: CHILD_OF-References → DEPENDS_ON-Kanten
3. Cypher-Query-Generierung und -Ausfuehrung
4. Graph-Visualisierungsdaten (Nodes + Relationships)

**Delegationspotential:**

| Aspekt | Bewertung |
|--------|-----------|
| Trace-Fetch | Bereits delegiert — einzelner GET `/traces/{traceId}` |
| Dependency-Extraktion | Nicht delegierbar — Jaeger liefert keine Dependency-Graphen pro Trace |
| Cypher-Query | Nicht delegierbar — nutzt Neo4j Graph-Struktur |
| Visualisierungsdaten | Nicht delegierbar — Neo4j-spezifisch |

**Performance:** o — Nur 1 Jaeger-Request pro Aufruf. Bottleneck ist die Cypher-Ausfuehrung, nicht Jaeger.

**Erweiterbarkeit:**
- Zentrales Feature fuer die Graph-Tracing-Integration
- Koennte auf andere Tracing-Backends erweitert werden (Zipkin, Tempo)
- Cypher-basierte Visualisierung ist einzigartig im Plugin

**Empfehlung: Im Plugin belassen** — Nicht delegierbar, Kern-Feature.

---

### 3.2 `timegraph.data.cypher_for_trace_subgraph_visualize_old` (JaegerTraceToSubgraphProcedure)

**Status:** Legacy/Deprecated, ersetzt durch `TraceToSubgraphProcedure_new`.

**Empfehlung: Kann entfernt werden (siehe C1 in optimization-proposals)**

---

## 4. Service-Abhaengigkeiten

### 4.1 `graphobs.jaeger.call_count_between` (JaegerDependencyProcedure)

**Aktuelle Logik:**
- HTTP-Request an Jaeger `/api/dependencies?endTs=...&lookback=...`
- Sucht spezifisches Parent-Child-Paar in den Ergebnissen
- Gibt Call Count zurueck (oder 0 wenn nicht gefunden)

**Plugin-seitige Verarbeitung:**
1. Parsing der Dependency-Response
2. Lineares Suchen nach dem angeforderten Service-Paar

**Delegationspotential:**

| Aspekt | Bewertung |
|--------|-----------|
| Dependency-Fetch | Bereits delegiert — Jaeger berechnet Dependencies serverseitig |
| Pair-Filterung | Trivial — koennte serverseitig sein, aber Jaeger bietet keinen Filter |

**Performance:** o — Nur 1 Jaeger-Request. Response ist typischerweise klein (Anzahl Service-Paare).

**Verbesserungspotential:**
- Hardcodierte Default-URL (`http://10.1.20.236:30212`) sollte durch Graph-Lookup ersetzt werden
- Listen-Variante koennte mehrere Paare in einem Aufruf abfragen (Response ist bereits vollstaendig)

**Empfehlung: Im Plugin belassen** — Bereits optimal delegiert. Kleine Code-Verbesserungen moeglich.

---

## 5. Import / Registrierung

### 5.1 `graphobs.jaeger.import_graph` (JaegerImportProcedure)

**Aktuelle Logik:**
- Auto-Detection der Jaeger API (probiert mehrere URL-Pfade)
- Holt alle Services via `/services`
- Holt Traces pro Service via `/traces?service=...`
- Extrahiert Services, Operations und Dependencies aus allen Traces
- Schreibt Graph in Neo4j (MERGE Service, Operation, DEPENDS_ON, HAS_OPERATION)
- Fuehrt ServicePodLinker aus

**Plugin-seitige Verarbeitung:**
1. API-Detection (6 URL-Varianten)
2. Trace-Analyse: Service→Operations→Dependencies-Graphaufbau
3. Neo4j-Write: Cypher MERGE-Operationen
4. ServicePodLinker: Verknuepfung mit Prometheus-registrierten Pods

**Delegationspotential:**

| Aspekt | Bewertung |
|--------|-----------|
| Service-Discovery | Bereits delegiert — `/services` Endpunkt |
| Trace-Fetch | Bereits delegiert — `/traces` pro Service |
| Graph-Aufbau | Nicht delegierbar — Neo4j-Write-Operationen |
| Pod-Linking | Nicht delegierbar — verbindet Jaeger- mit Prometheus-Daten |
| Parallelisierung | Verbesserbar — Trace-Fetches pro Service koennten parallel laufen (A3) |

**Performance:** + — Bei vielen Services (z.B. 30) koennten parallele Requests den Import
beschleunigen. Nicht durch Delegation, sondern durch Parallelisierung im Plugin.

**Empfehlung: Im Plugin belassen, Parallelisierung erwaegen (A3)**

---

## Zusammenfassung

### Uebersichtstabelle

| # | Procedure | Plugin-Logik | Delegierbar? | Perf. | Erweiterbarkeit | Empfehlung |
|---|-----------|-------------|--------------|-------|-----------------|------------|
| 1.1 | `get_trace` | Op/Status Filter, Metriken | Minimal | o | Flexibel | Plugin |
| 1.2 | `get_traces_for_services` | Multi-Service, Dedup | Nein | + | Flexibel | Plugin (+Parallel) |
| 1.3 | `get_traces_for_subgraph` | ALL-Match Subgraph | Nein | -- | Kern-Feature | Plugin |
| 2.1 | `get_spans` | Span-Extraktion, Tag-Merge | Bereits optimal | o | Flexibel | Plugin |
| 2.2 | `get_spans_for[_nodes]` | Node→Service Bridge | Nein | o | Kern-Feature | Plugin |
| 3.1 | `cypher_for_trace_*` | Cypher-Visualisierung | Nein | o | Kern-Feature | Plugin |
| 4.1 | `call_count_between` | Pair-Lookup | Bereits optimal | o | Flexibel | Plugin |
| 5.1 | `import_graph` | API-Detection, Graph-Build | Nein | + | Kern-Feature | Plugin (+Parallel) |

### Fazit

**Keine der Jaeger-Procedures sollte an Jaeger delegiert werden.** Im Gegensatz zu Prometheus,
das mit PromQL eine maechtige serverseitige Abfragesprache bietet, ist die Jaeger Query API
primaer ein Daten-Abruf-Interface ohne Aggregation oder komplexe Filterung.

Die Plugin-seitige Verarbeitung faellt in drei Kategorien:

1. **Graph-Integration** (nicht delegierbar): Node-Aufloesung, Cypher-Queries, ServicePodLinker —
   diese Logik verbindet Jaeger-Daten mit dem Neo4j-Graphen und ist die Raison d'etre des Plugins.

2. **Flexibles Filtering** (bewusst im Plugin): Tag-Varianten-Matching, Substring-Operations,
   ALL-Match-Semantik — diese Logik macht das Plugin robust gegenueber verschiedenen
   Jaeger-Versionen und Instrumentierungskonventionen.

3. **Multi-Source-Vorbereitung** (strategisch): Die Plugin-seitige Span-Extraktion und
   Tag-Normalisierung ermoeglicht zukuenftige Integration anderer Tracing-Backends
   (Zipkin, Grafana Tempo, AWS X-Ray) ohne API-Aenderungen.

### Empfohlene Optimierungen (ohne Delegation)

| Optimierung | Impact | Aufwand |
|---|---|---|
| **A3: Parallelisierung** der Multi-Service-Requests (CompletableFuture) | Mittel-Hoch | Klein |
| **Hardcodierte URL** in JaegerDependencyProcedure durch Graph-Lookup ersetzen | Sauberkeit | Trivial |
| **Listen-Variante** fuer `call_count_between` (mehrere Paare, 1 Request) | Klein | Klein |
| **Caching** der Jaeger baseUrl (statt pro Aufruf Graph-Lookup) | Minimal | Trivial |
