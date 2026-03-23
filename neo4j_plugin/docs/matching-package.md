# Matching-Package (`graphobs.matching`)

## Motivation

In einem Observability-Stack liefern verschiedene Datenquellen (Jaeger, Prometheus, OpenSearch) Informationen uber dieselben Entitaten -- Services, Pods, Server. Das Problem: Jede Quelle benennt diese Entitaten anders.

| Entitat | Prometheus | Jaeger | OpenSearch (OTel) |
|---------|-----------|--------|-------------------|
| Pod | `pod`, `pod_name`, `kubernetes_pod_name`, ... | `pod.name`, `pod_name`, `kubernetes.pod.name` | `resource.k8s.pod.name` |
| Service | `service_name` (OTel spanmetrics) | `serviceName` (Prozess-Ebene) | `resource.service.name` |
| Server | `instance`, `node` | (nicht direkt) | (nicht direkt) |
| Operation | `operation`, `span_name` | `operationName` (Span-Ebene) | (nicht direkt) |
| Severity | (nicht relevant) | (nicht relevant) | `severity.text`, `severity_text`, `level`, `log.level` |
| Message | (nicht relevant) | (nicht relevant) | `body`, `message`, `msg`, `log` |

Ohne ein zentrales Matching-Package muesste jede Procedure ihre eigene Feld-Aufloesung implementieren. Das fuehrt zu Duplikation, Inkonsistenzen und erschwert die Wartung bei neuen Datenquellen.

## Architektur

```
matching/
├── FieldNameResolver.java      // Zentrale Feld-Aufloesung + gespeicherte Metadaten
├── OperationIdParser.java      // Kanonisches ID-Format fuer Operationen
├── EntityNameNormalizer.java   // Namens-Normalisierung
├── NodeResolver.java           // Graph-Traversal (Server->Pods, Node->Service)
└── ServicePodMatcher.java      // Graph-Verlinkung Service <-> Pod
```

---

## Label-Management: Discover, Store, Use

### Das Grundprinzip

Frueherer Ansatz: Bei jeder Query eine Kandidatenliste durchprobieren ("raten").

Neuer Ansatz: **Discover → Store → Use → Fallback**

1. **Discover**: Bei der Registrierung (`register_prometheus`, `jaeger.import_graph`, `register_opensearch`) werden die tatsaechlich vorhandenen Label-/Tag-/Feldnamen erkannt.
2. **Store**: Das Ergebnis wird als Property auf dem jeweiligen Datenquellen-Knoten im Graph gespeichert.
3. **Use**: Bei Queries wird zuerst die gespeicherte Property gelesen. Wenn vorhanden, wird nur dieses eine Feld abgefragt.
4. **Fallback**: Wenn die Property fehlt (alter Graph ohne Metadaten), greift das bisherige Rateverhalten mit Kandidatenlisten.

### Graph-Schema: Gespeicherte Metadaten

```
(:Prometheus {
    url: "http://...",
    names: [...],
    podLabel: "pod",          // Entdeckter Pod-Label-Name
    serverLabel: "node"       // Entdeckter Server-Label-Name
})

(:Jaeger {
    baseUrl: "http://...",
    podTagKey: "kubernetes.pod.name"  // Entdeckter Pod-Tag-Key
})

(:LogIndex {
    name: "otel-logs-2024.01",
    docCount: ...,
    fieldCount: ...,
    podField: "resource.k8s.pod.name",     // Aufgeloestes Pod-Feld
    serviceField: "resource.service.name",  // Aufgeloestes Service-Feld
    severityField: "severity.text",         // Aufgeloestes Severity-Feld
    messageField: "body"                    // Aufgeloestes Message-Feld
})
```

---

## Vollstaendige Label-Inventarliste

### Prometheus

| Entitat | Kontext | Labels (Prioritaet) | Discover-Zeitpunkt |
|---------|---------|---------------------|-------------------|
| **Pod** | Registrierung: Discovery | `pod`, `pod_name`, `kubernetes_pod_name`, `container`, `container_name`, `name`, `container_label_io_kubernetes_pod_name`, `exported_pod`, `pod_id`, `com_docker_compose_service` | `fetchFirstAvailableLabelValuesAndLabel()` — iteriert Kandidaten, erster mit Ergebnissen gewinnt |
| **Pod** | Query: Zeitreihen-Abfrage | `name`, `pod`, `pod_name`, `instance`, `host`, `hostname`, `container`, `job` | Nutzt gespeichertes `podLabel` oder faellt auf OR-Kette zurueck |
| **Server** | Registrierung | `node` (primaer), `instance` (fallback) | `fetchServerNamesWithLabel()` — probiert `node` zuerst, dann `instance` |
| **Server** | Time-Range-Queries | `instance` (hardcodiert als `SERVER_LABEL`) | Statisch — nur fuer Batch-Ranges im HttpClient |
| **Pod-Server-Mapping** | Registrierung | `pod` + `instance` (hardcodiert) | `fetchPodToServerMapping()` — beruht auf Prometheus-Metrik-Struktur |
| **Service** | Query | `service_name` (hardcodiert) | `buildComplexPromQL()` — OTel-Standard, aktuell nicht dynamisch |
| **Operation** | Query | `operation`, `span_name` | `buildComplexPromQL()` — OR ueber beide Varianten |

### Jaeger

| Entitat | Kontext | Tag-Keys | Discover-Zeitpunkt |
|---------|---------|----------|-------------------|
| **Pod** | Post-Fetch-Filter | `pod.name`, `pod_name`, `kubernetes.pod.name` | `extractGraphData()` — waehrend Import werden alle Span-/Process-Tags gescannt |
| **Status** | Trace-Filter | `http.status_code`, `status.code`, `status` | Hardcodiert in `fetchTracesRaw()` — kein Discovery noetig |
| **Service** | Import | `process.serviceName` (Jaeger-nativ) | Kein Tag — fester Jaeger-API-Pfad |
| **Operation** | Import | `span.operationName` (Jaeger-nativ) | Kein Tag — fester Jaeger-API-Pfad |

### OpenSearch

| Entitat | Kontext | Feldnamen | Discover-Zeitpunkt |
|---------|---------|-----------|-------------------|
| **Pod** | Query-Filter | `.keyword`-Variante des gespeicherten Felds, Fallback: `resource.k8s.pod.name.keyword` | `getFieldMappings()` bei Registrierung |
| **Pod** | Ergebnis-Extraktion | `resource.k8s.pod.name`, `kubernetes.pod.name`, `kubernetes.pod_name`, `pod.name`, `container.name` | `getFieldMappings()` bei Registrierung |
| **Service** | Query-Filter | `.keyword`-Variante des gespeicherten Felds, Fallback: `resource.service.name.keyword` | `getFieldMappings()` bei Registrierung |
| **Service** | Ergebnis-Extraktion | `resource.service.name`, `service.name`, `kubernetes.labels.app`, `application` | `getFieldMappings()` bei Registrierung |
| **Severity** | Log-Parsing | `severity.text`, `severity_text`, `level`, `log.level` | `getFieldMappings()` bei Registrierung |
| **Message** | Log-Parsing | `body`, `message`, `msg`, `log` | `getFieldMappings()` bei Registrierung |

---

## Wo muss geraten werden?

| Situation | Raten noetig? | Begruendung | Was passiert danach? |
|-----------|--------------|-------------|---------------------|
| Prometheus Pod-Label | **Ja, einmalig** | Wir wissen nicht, welches Label der Cluster nutzt. `fetchFirstAvailableLabelValuesAndLabel()` probiert die Kandidatenliste durch. | Ergebnis wird als `podLabel` auf `:Prometheus` gespeichert. Alle folgenden Queries nutzen direkt dieses Label. |
| Prometheus Server-Label | **Ja, einmalig** | `node` vs `instance` muss einmal ausprobiert werden. `fetchServerNamesWithLabel()` probiert `node` zuerst. | Ergebnis wird als `serverLabel` auf `:Prometheus` gespeichert. |
| Jaeger Pod-Tag-Key | **Nein** | Beim Import iterieren wir bereits ueber alle Spans und deren Tags. Wir lesen die Keys einfach mit und speichern den ersten Treffer aus der Kandidatenliste. | Ergebnis wird als `podTagKey` auf `:Jaeger` gespeichert. |
| OpenSearch Feldnamen | **Nein** | `getFieldMappings()` wird bei der Registrierung aufgerufen und liefert alle Felder des Index. Wir pruefen die Kandidaten gegen dieses Mapping. | Ergebnis wird als `podField`, `serviceField`, `severityField`, `messageField` auf `:LogIndex` gespeichert. |
| Service-Pod-Matching | **Ja, immer** | Beruht auf Kubernetes-Namenskonvention (`pod.name STARTS WITH service.name + "-"`). Kein Label kann das ersetzen — es gibt kein universelles "belongs-to-service" Feld. | `ServicePodMatcher` laeuft bei jeder Registrierung und jedem Updater-Zyklus. |
| Prometheus `service_name` | **Nein (aber hardcodiert)** | OTel spanmetrics nutzen `service_name`. Aktuell nicht dynamisch aufgeloest, da es keinen Discovery-Mechanismus dafuer gibt. | Koennte in Zukunft ebenfalls gespeichert werden. |
| Prometheus Operation-Label | **Ja, bei jeder Query** | `operation` vs `span_name` — wird per OR-Kette abgefragt. Kein sinnvoller Discovery-Zeitpunkt, da Operations aus Jaeger kommen und Prometheus-Labels erst bei konkreter Metrik bekannt sind. | OR-Kette mit 2 Varianten ist akzeptabel (geringer Overhead). |

---

## Fallback-Strategie im Detail

### Grundprinzip

```java
String storedLabel = node.getProperty("podLabel", null);
if (storedLabel != null) {
    // Optimiert: eine einzelne Query
    query = String.format("%s{%s=\"%s\"}", metric, storedLabel, targetName);
} else {
    // Fallback: alte OR-Kette
    log.info("Prometheus node lacks stored podLabel metadata. "
           + "Using multi-label fallback. Re-run register_prometheus to optimize.");
    query = buildMultiLabelQuery(metric, targetName);
}
```

### Prometheus: Pod-Zeitreihen (`PrometheusTimeSeriesSource`)

**Mit gespeichertem Label** (nach Registrierung):
```promql
container_cpu_usage{pod="frontend-6f8b7c5-abc12"}
```

**Ohne gespeichertes Label** (alter Graph, Fallback):
```promql
container_cpu_usage{name="frontend-6f8b7c5-abc12"}
  or container_cpu_usage{pod="frontend-6f8b7c5-abc12"}
  or container_cpu_usage{pod_name="frontend-6f8b7c5-abc12"}
  or container_cpu_usage{instance="frontend-6f8b7c5-abc12"}
  or container_cpu_usage{host="frontend-6f8b7c5-abc12"}
  or container_cpu_usage{hostname="frontend-6f8b7c5-abc12"}
  or container_cpu_usage{container="frontend-6f8b7c5-abc12"}
  or container_cpu_usage{job="frontend-6f8b7c5-abc12"}
```

**Betroffene Methoden**: `fetch()`, `fetchAggregated()`, `fetchBatch()` — alle drei pruefen `sourceNode.getProperty("podLabel", null)` und waehlen den optimierten oder den Fallback-Pfad.

### Jaeger: Pod-Filter in Traces (`JaegerGetTraceProcedure`)

**Mit gespeichertem Tag-Key** (nach Import):
```java
// Nur den einen bekannten Key pruefen
if ("kubernetes.pod.name".equals(tag.getKey()) && podNames.contains(tag.getValue())) {
    return true;
}
```

**Ohne gespeichertem Tag-Key** (alter Graph, Fallback):
```java
// Alle 3 Kandidaten durchprobieren
for (String podTagKey : JAEGER_POD_TAG_KEYS) {
    if (podTagKey.equals(tag.getKey()) && podNames.contains(tag.getValue())) {
        return true;
    }
}
```

### OpenSearch: Log-Parsing (`OpenSearchGetLogsProcedure`)

**Mit gespeicherten Feldern** (nach Registrierung):
```java
// Severity: direkt das bekannte Feld nutzen
String level = getNestedJsonText(source, "severity.text"); // gespeichertes Feld
```

**Ohne gespeicherte Felder** (alter Graph, Fallback):
```java
// Severity: Kandidatenliste durchprobieren
// 1. severity.text (OTel-Objekt)
// 2. severity_text (flach)
// 3. level
// 4. log.level
```

Analog fuer `messageField`, `podField`, `serviceField`.

### OpenSearch: Query-Filter (`FieldNameResolver`)

**Mit gespeichertem Feld** (nach Registrierung):
```
Pod-Filter:  kubernetes.pod.name.keyword = "frontend-abc"
```

**Ohne gespeichertes Feld** (alter Graph, Fallback):
```
Pod-Filter:  resource.k8s.pod.name.keyword = "frontend-abc"  (hardcodierter Default)
```

Die index-bewusste Aufloesung wird ueber `resolveNodeToOpenSearchFilter(node, db, indexName)` aktiviert. Ohne `db`/`indexName` Parameter (alter Code-Pfad) greift der hardcodierte Default.

---

## Backward-Kompatibilitaet

- `node.getProperty("podLabel", null)` liefert `null` bei alten Graphen ohne Metadaten
- `null` → bisheriges Rateverhalten mit Kandidatenlisten
- **Keine Migration noetig**: Alte Graphen funktionieren weiter mit dem Fallback
- **Erneutes Ausfuehren der Registrierung** fuegt die Metadaten automatisch hinzu
- Properties werden nur gesetzt, wenn der Discovery-Wert nicht `null` ist (`CASE WHEN $podLabel IS NOT NULL THEN $podLabel ELSE pr.podLabel END`)

---

## Komponenten im Detail

### 1. FieldNameResolver

Kernkomponente: Loest Feldnamen quellenuebergreifend auf.

**Kandidatenlisten** (probiert in Reihenfolge, erster Treffer gewinnt):

```java
// Prometheus: 10 Varianten fuer Pod-Labels
PROMETHEUS_POD_LABELS = {"pod", "pod_name", "kubernetes_pod_name", ...}

// Jaeger: 3 Varianten in Span/Process-Tags
JAEGER_POD_TAG_KEYS = {"pod.name", "pod_name", "kubernetes.pod.name"}

// OpenSearch: OTel-spezifische Pfade
OPENSEARCH_POD_FIELDS = {"resource.k8s.pod.name", "kubernetes.pod.name", ...}
OPENSEARCH_SERVICE_FIELDS = {"resource.service.name", "service.name", ...}
```

**Zentralisierte Aufloesung ueber gespeicherte Metadaten:**

```java
// Prometheus: Gespeichertes Label vom Prometheus-Knoten lesen
FieldNameResolver.resolvePrometheusLabel(prometheusNode, "pod")    // -> "pod" oder null
FieldNameResolver.resolvePrometheusLabel(prometheusNode, "server") // -> "node" oder null

// Jaeger: Gespeicherten Pod-Tag-Key vom Jaeger-Knoten lesen
FieldNameResolver.resolveJaegerPodTagKey(db)                       // -> "kubernetes.pod.name" oder null

// OpenSearch: Gespeicherte Felder vom LogIndex-Knoten lesen
FieldNameResolver.resolveOpenSearchField("otel-logs-*", "pod", db)      // -> "resource.k8s.pod.name" oder null
FieldNameResolver.resolveOpenSearchField("otel-logs-*", "severity", db) // -> "severity.text" oder null
```

**Verschachtelte Feld-Aufloesung** -- das kniffligste Problem:

OpenSearch-Dokumente haben eine gemischte Struktur. Das Feld `resource.service.name` ist *nicht* `resource` -> `service` -> `name`, sondern `resource` -> `"service.name"` (Key mit Punkt). Die `getNestedField()`-Methode probiert daher alle moeglichen Split-Punkte rekursiv:

```java
// Eingabe: key = "resource.service.name"
// Versuch 1: map["resource.service.name"]          -> miss
// Versuch 2: map["resource"]["service.name"]        -> HIT!
// Versuch 3: map["resource"]["service"]["name"]     -> miss (wuerde auch probiert)
```

**Node-zu-OpenSearch-Mapping** fuer `get_logs_for`:

```java
resolveNodeToOpenSearchFilter(node):
  Service-Node  -> {id}       -> serviceField.keyword (oder Fallback: resource.service.name.keyword)
  Pod-Node      -> {name}     -> podField.keyword     (oder Fallback: resource.k8s.pod.name.keyword)
  Operation-Node -> {id}      -> OperationIdParser.extractServiceName() -> serviceField.keyword

resolveNodeToOpenSearchFilter(node, db, indexName):
  // Index-bewusste Variante: laedt gespeicherte Felder vom LogIndex-Knoten
```

### 2. OperationIdParser

Definiert ein kanonisches ID-Format fuer Operationen: `service::operation`.

```java
OperationIdParser.buildOperationId("frontend", "GET /api/cart")
// -> "frontend::GET /api/cart"

OperationIdParser.extractServiceName("frontend::GET /api/cart")
// -> "frontend"

OperationIdParser.extractOperationName("frontend::GET /api/cart")
// -> "GET /api/cart"
```

**Warum `::` als Separator?**
- Kommt weder in Service-Namen noch in HTTP-Pfaden vor
- Eindeutig parsebar (im Gegensatz zu `/` oder `.`)
- Gut lesbar in Cypher-Queries

### 3. EntityNameNormalizer

Bereinigt Namen aus verschiedenen Quellen fuer konsistenten Vergleich:

```java
EntityNameNormalizer.normalizePodName("default/frontend-6f8b7c5-abc12")
// -> "frontend-6f8b7c5-abc12"  (Namespace-Prefix entfernt)
```

### 4. ServicePodMatcher

Verlinkt Service- und Pod-Knoten im Graph automatisch. Nutzt eine Longest-Match-Strategie:

```
Pod: "frontend-proxy-6bb94d786f-5l98l"
Service "frontend-proxy" -> STARTS WITH match (Laenge 14) -- GEWINNT
Service "frontend"       -> STARTS WITH match (Laenge 8)
```

**Algorithmus:**
1. Primaer: `pod.name STARTS WITH service.name + "-"`, laengster Match zuerst
2. Fallback: `pod.name CONTAINS service.name` fuer nicht gematchte Pods
3. Ergebnis: `(:Pod)-[:BELONGS_TO]->(:Service)` Relationen

### 5. NodeResolver

Loest Graph-Knoten auf ihre zugehoerigen Entitaeten auf:

- `resolveServerToPodNames(serverNode, db)` — findet alle Pods auf einem Server via `[:DEPLOYED_ON]`
- `resolveOwningServiceNames(elementIds, db)` — findet den zugehoerigen Service fuer Pods/Operations via Graph-Traversal

---

## Zusammenspiel mit den Datenquellen

```
                    FieldNameResolver
              (Kandidatenlisten + gespeicherte Metadaten)
                          |
         +----------------+----------------+
         |                |                |
    Prometheus       Jaeger           OpenSearch
    podLabel         podTagKey        podField, serviceField
    serverLabel                       severityField, messageField
         |                |                |
    Registrierung    Import           Registrierung
    speichert        speichert        speichert
    Labels           Tag-Keys         Feld-Mappings
         |                |                |
         v                v                v
    (:Prometheus)    (:Jaeger)        (:LogIndex)
         \              |              /
          \             |             /
           +---> Neo4j Graph <------+
                ServicePodMatcher
                EntityNameNormalizer
                OperationIdParser
                NodeResolver
```

### Datenfluss bei einer Query

```
1. User ruft z.B. get_timeseries(pod, metric) auf
2. PrometheusTimeSeriesSource liest sourceNode.podLabel
3a. podLabel vorhanden  -> metric{pod="xyz"}           (1 Query)
3b. podLabel fehlt      -> metric{name="xyz"} or ...   (8-fach OR, Fallback)
4. Prometheus antwortet
5. Ergebnis wird zurueckgegeben
```

```
1. User ruft get_logs_for(pod, index) auf
2. OpenSearchGetLogsProcedure laedt IndexFieldMeta vom LogIndex-Knoten
3. FieldNameResolver nutzt gespeichertes podField (oder Fallback)
4. extractLevel() / extractMessage() nutzen gespeicherte Felder (oder Kandidatenlisten)
5. OpenSearch antwortet
6. Ergebnis wird geparsed und zurueckgegeben
```

Die Grundidee: Jede Datenquelle liefert Entitaeten unter verschiedenen Namen. Das Matching-Package normalisiert und verknuepft sie zu einem einheitlichen Graph. Die Registrierung entdeckt die korrekten Namen und speichert sie, sodass Queries gezielt statt breit abfragen koennen.
