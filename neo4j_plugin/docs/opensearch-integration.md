# OpenSearch-Integration

## Motivation

Die OpenTelemetry Demo erzeugt neben Traces (Jaeger) und Metriken (Prometheus) auch Logs, die in OpenSearch landen. Um alle drei Observability-Saeulen ueber den Neo4j-Graph abfragbar zu machen, wurde eine OpenSearch-Anbindung implementiert -- analog zu den bestehenden Jaeger- und Prometheus-Integrationen.

**Ziel:** Logs sollen sowohl direkt (Index + Filter) als auch ueber Graph-Knoten (Service, Pod, Operation) abfragbar sein.

## Architektur

```
source_registration/opensearch_registration/
├── OpenSearchClient.java              // Interface
├── OpenSearchHttpClient.java          // HTTP-Implementierung
├── LogRepository.java                 // Interface fuer Neo4j-Ops
├── Neo4jLogRepository.java            // Cypher-Implementierung
├── OpenSearchRegistrar.java           // Procedure: register_opensearch
├── OpenSearchRegistrationService.java // Registrierungs-Logik
└── OpenSearchToNeo4jUpdater.java      // Periodische Index-Synchronisation

trace/sources/
└── OpenSearchGetLogsProcedure.java    // Procedures: get_logs + get_logs_for
```

### Design-Entscheidungen

**1. Live-Abfrage statt Import**

Im Gegensatz zu Prometheus (wo Pod/Server-Zustaende in den Graph importiert werden) werden Logs *nicht* in Neo4j gespeichert. Jeder `get_logs`-Aufruf fragt live bei OpenSearch an. Gruende:

- Logs sind hochvolumig (zehntausende pro Stunde) -- Import waere zu teuer
- OpenSearch ist fuer Volltextsuche optimiert -- Neo4j nicht
- Aktualitaet: Kein Sync-Delay, immer die neuesten Logs

**2. Optionale Basic Auth**

Die OpenTelemetry Demo nutzt OpenSearch teilweise mit Basic Auth (`admin/admin`). Die Implementierung unterstuetzt optionale Credentials, die auf dem `(:OpenSearch)`-Knoten gespeichert werden, sodass `get_logs` sie automatisch liest.

**3. Case-insensitiver Level-Filter**

OTel-Logs haben inkonsistente Level-Bezeichnungen (`"INFO"`, `"info"`, `"Information"`, `"Error"`). Der Level-Filter nutzt daher `match`-Queries (case-insensitive) statt `term`-Queries (exakt), und prueft sowohl `severity.text` (OTel-Objektformat) als auch `severity_text` (Flat-Format).

**4. Graph-basierte Abfrage via `get_logs_for`**

Der eigentliche Mehrwert: Statt OpenSearch-Feldnamen zu kennen, uebergibt man einen Graph-Knoten. Die Procedure nutzt `FieldNameResolver.resolveNodeToOpenSearchFilter()`, um den Knoten auf das richtige OpenSearch-Feld abzubilden. Details dazu im Dokument `matching-package.md`.

---

## Graph-Schema

```
(:OpenSearch {url, indexPattern, indexCount, user, password})
(:LogIndex {name, docCount, fieldCount})-[:STORED_IN]->(:OpenSearch)
```

Der `(:OpenSearch)`-Knoten speichert die Verbindungsdaten. `(:LogIndex)`-Knoten bilden die vorhandenen Indices ab (werden bei Registrierung und optionalem periodischen Update synchronisiert).

---

## Procedures

### 1. Registrierung: `graphobs.datasources.register_opensearch`

Registriert eine OpenSearch-Instanz und legt den `(:OpenSearch)`-Knoten an.

```cypher
-- Minimal
CALL graphobs.datasources.register_opensearch("http://opensearch:9200", {index: "otel-logs-*"})

-- Mit Basic Auth
CALL graphobs.datasources.register_opensearch("http://opensearch:9200", {
  index: "otel-logs-*",
  user: "admin",
  password: "admin"
})

-- Mit periodischem Index-Update
CALL graphobs.datasources.register_opensearch("http://opensearch:9200", {
  index: "otel-logs-*",
  update: true,
  intervalSeconds: 60
})
```

**Options:**

| Key | Typ | Default | Beschreibung |
|-----|-----|---------|-------------|
| `index` | String | `"*"` | Index-Pattern (z.B. `"otel-logs-*"`) |
| `user` | String | - | Basic Auth Username |
| `password` | String | - | Basic Auth Password |
| `update` | Boolean | `false` | Periodische Index-Synchronisation |
| `intervalSeconds` | Number | `60` | Update-Intervall in Sekunden |

**Return:** `message`, `url`, `indexCount`

**Auto-Start via Umgebungsvariablen:**

Wird beim Neo4j-Start automatisch ausgefuehrt, wenn gesetzt:

| Variable | Beschreibung |
|----------|-------------|
| `OPENSEARCH_URL` | Basis-URL (z.B. `http://opensearch:9200`) |
| `OPENSEARCH_USER` | Basic Auth Username (optional) |
| `OPENSEARCH_PASSWORD` | Basic Auth Password (optional) |
| `OPENSEARCH_INDEX` | Index-Pattern, Default `"*"` (optional) |

---

### 2. Direkte Log-Abfrage: `graphobs.data.get_logs`

Fragt Logs direkt per Index und Filter ab.

```cypher
CALL graphobs.data.get_logs("otel-logs-*", {
  level: "ERROR",
  range: "-1h",
  limit: 50
})
YIELD timestamp, level, message, source, fields
```

**Parameter:**

| Key | Typ | Default | Beschreibung |
|-----|-----|---------|-------------|
| `time` | Number/String | jetzt | Endzeitpunkt (Epoch-Millis oder ISO-String) |
| `range` | String | `"-1h"` | Zeitfenster (`-30m`, `-2h`, `-1d`, ...) |
| `limit` | Number | `100` | Max. Ergebnisse |
| `level` | String | - | Log-Level (case-insensitive: `error`, `ERROR`, `Error`) |
| `query` | String | - | Volltextsuche |
| `dsl` | String | - | Raw OpenSearch Query DSL (JSON-String) |
| *andere Keys* | String | - | Feld-Filter mit `.keyword`-Matching |

**Return-Felder:**

| Feld | Typ | Beschreibung |
|------|-----|-------------|
| `timestamp` | String | ISO-Zeitstempel (`@timestamp`) |
| `level` | String | Log-Level aus `severity.text` (kann `null` sein) |
| `message` | String | Log-Nachricht aus `body` |
| `source` | String | Service- oder Pod-Name (via FieldNameResolver) |
| `fields` | Map | Alle Felder des Dokuments |

#### Beispiele

```cypher
-- Volltextsuche
CALL graphobs.data.get_logs("otel-logs-*", {query: "timeout OR connection refused"})
YIELD timestamp, level, message, source

-- Feld-Filter (Backticks fuer Keys mit Punkten!)
CALL graphobs.data.get_logs("otel-logs-*", {
  `resource.service.name`: "accounting",
  level: "error",
  range: "-2h"
})
YIELD timestamp, message

-- Raw DSL fuer Power-User
CALL graphobs.data.get_logs("otel-logs-*", {
  dsl: '{"size":5,"query":{"match_all":{}}}'
})
YIELD timestamp, level, message
```

---

### 3. Graph-basierte Log-Abfrage: `graphobs.data.get_logs_for`

Der Kern-Mehrwert: Logs ueber Graph-Knoten abfragen, ohne OpenSearch-Feldnamen zu kennen.

```cypher
CALL graphobs.data.get_logs_for(node, index, params)
YIELD timestamp, level, message, source, fields
```

**Node-Mapping:**

| Node-Label | Gelesene Property | OpenSearch-Filter |
|------------|-------------------|-------------------|
| `Service` | `id` | `resource.service.name.keyword = id` |
| `Pod` | `name` | `resource.k8s.pod.name.keyword = name` |
| `Operation` | `id` -> Service extrahiert | `resource.service.name.keyword = service` |

Die Zuordnung wird zentral im `FieldNameResolver` verwaltet (siehe `matching-package.md`).

#### Beispiele

```cypher
-- Logs fuer einen Service
MATCH (s:Service {id: "product-catalog"})
CALL graphobs.data.get_logs_for(s, "otel-logs-*", {level: "info", range: "-30m", limit: 10})
YIELD timestamp, level, message, source
RETURN timestamp, level, message, source

-- Logs fuer einen Pod
MATCH (p:Pod {name: "accounting-5d4b8c7f9-x2k8m"})
CALL graphobs.data.get_logs_for(p, "otel-logs-*", {range: "-1h"})
YIELD timestamp, level, message
RETURN timestamp, level, message

-- Logs fuer eine Operation (filtert automatisch nach dem zugehoerigen Service)
MATCH (o:Operation {id: "checkout::POST /api/orders"})
CALL graphobs.data.get_logs_for(o, "otel-logs-*", {level: "error", range: "-6h"})
YIELD timestamp, message
RETURN timestamp, message

-- Graph-Traversierung: Error-Logs aller Services, die "checkout" aufruft
MATCH (s:Service {id: "checkout"})-[:CALLS]->(dep:Service)
CALL graphobs.data.get_logs_for(dep, "otel-logs-*", {level: "error", range: "-1h"})
YIELD timestamp, message, source
RETURN source AS service, count(*) AS errors, collect(message)[..3] AS samples
ORDER BY errors DESC

-- Korrelation: Pods eines Services mit ihren Logs
MATCH (s:Service {id: "cart"})<-[:BELONGS_TO]-(p:Pod)
CALL graphobs.data.get_logs_for(p, "otel-logs-*", {level: "error", range: "-2h"})
YIELD timestamp, message
RETURN p.name AS pod, count(*) AS errorCount
```

---

## OTel-Dokumentstruktur

Die OpenTelemetry Demo schreibt Logs im folgenden Format nach OpenSearch:

```json
{
  "@timestamp": "2026-02-13T10:05:04.416514453Z",
  "body": "Product Found",
  "severity": {
    "text": "INFO",
    "number": 9
  },
  "resource": {
    "service.name": "product-catalog",
    "k8s.pod.name": "product-catalog-589bcf4889-df4pv",
    "k8s.deployment.name": "product-catalog",
    "k8s.namespace.name": "default",
    "k8s.node.name": "gke-cluster-1-default-pool-201cc300-1dx0"
  },
  "attributes": {
    "app.product.id": "LS4PSXUNUM"
  },
  "traceId": "44136c5d578ebfce9167d57b36223858",
  "spanId": "7ecd943b83841a41",
  "instrumentationScope": {
    "name": "product-catalog"
  }
}
```

Besonderheiten:
- `severity` ist ein **Objekt** (nicht ein flacher String) -- daher die spezielle Extraktion
- Keys in `resource` enthalten **Punkte** (`"service.name"`) -- daher die rekursive Feld-Aufloesung im FieldNameResolver
- Level-Werte sind **nicht einheitlich**: `"INFO"`, `"info"`, `"Information"`, `"Error"` -- daher der case-insensitive Filter
- Manche Logs (z.B. Envoy-Proxy) haben `"severity": {}` -- dort ist `level` = `null`

---

## Hinweis: Verfuegbare Services in OpenSearch

Nicht alle Services aus dem Jaeger-Graph haben Logs in OpenSearch. Aktuell loggen folgende Services:

`product-catalog`, `frontend-proxy`, `load-generator`, `kafka`, `cart`, `checkout`, `ad`, `shipping`, `currency`, `payment`, `recommendation`, `accounting`, `fraud-detection`, `quote`

Insbesondere hat `frontend` (die Browser-App) **keine** Logs in OpenSearch -- nur `frontend-proxy` (der Envoy-Reverse-Proxy).
