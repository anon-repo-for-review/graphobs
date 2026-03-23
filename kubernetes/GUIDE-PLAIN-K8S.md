# TimeGraph Integration - Plain Kubernetes

Diese Anleitung beschreibt die Integration von TimeGraph in die OpenTelemetry Demo unter Verwendung des **Plain Kubernetes Manifests** (ohne Helm).

**Verwendetes Manifest:** `timegraph-extension.yaml`

## Voraussetzungen

- Docker installiert
- kubectl konfiguriert für deinen Cluster
- Maven installiert (für den Java-Build)

## Schritt 1: Docker Image bauen

### 1.1 Maven Build

```bash
cd /path/to/timegraph
cd neo4j_plugin
mvn clean package -DskipTests
cd ..
```

Das erzeugt `neo4j_plugin/target/neo4j_plugin-1.0-SNAPSHOT.jar`.

### 1.2 Docker Image bauen

```bash
# Mit Build-Script
./build-docker.sh

# Oder manuell
docker build -t timegraph-neo4j:latest .
```

### 1.3 Image in Registry pushen (für Kubernetes)

```bash
# Tag für deine Registry
docker tag timegraph-neo4j:latest <deine-registry>/timegraph-neo4j:latest

# Push
docker push <deine-registry>/timegraph-neo4j:latest
```

**Hinweis:** Für lokale Tests mit Minikube:
```bash
eval $(minikube docker-env)
docker build -t timegraph-neo4j:latest .
```

## Schritt 2: OpenTelemetry Demo Manifest herunterladen

```bash
curl -O https://raw.githubusercontent.com/open-telemetry/opentelemetry-demo/main/kubernetes/opentelemetry-demo.yaml
```

## Schritt 3: TimeGraph integrieren

### Option A: An Original-Manifest anhängen (empfohlen)

```bash
# TimeGraph an das OTel Demo Manifest anhängen
cat kubernetes/timegraph-extension.yaml >> opentelemetry-demo.yaml

# Alles zusammen deployen
kubectl apply -f opentelemetry-demo.yaml
```

### Option B: Separat deployen

```bash
# Erst OTel Demo deployen
kubectl apply -f opentelemetry-demo.yaml

# Dann TimeGraph (im gleichen Namespace)
kubectl apply -f kubernetes/timegraph-extension.yaml
```

## Schritt 4: Installation verifizieren

### Namespace prüfen

Das OTel Demo Plain-Manifest verwendet den Namespace `otel-demo`:

```bash
kubectl get pods -n otel-demo
```

### TimeGraph Pod-Status

```bash
kubectl get pods -l app.kubernetes.io/component=timegraph -n otel-demo
```

Erwartete Ausgabe:
```
NAME                         READY   STATUS    RESTARTS   AGE
timegraph-xxxxxxxxx-xxxxx    1/1     Running   0          2m
```

### Logs prüfen (Auto-Registrierung)

```bash
kubectl logs -l app.kubernetes.io/component=timegraph -n otel-demo | grep -i "auto-"
```

Erwartete Log-Einträge nach ~30 Sekunden:
```
Auto-registering Prometheus data source: http://prometheus:9090
Prometheus registration complete. Registered X pods.
Auto-importing Jaeger graph: http://jaeger-query:16686
Jaeger import result: Success (services=X, operations=X, dependencies=X)
```

## Schritt 5: Auf Neo4j zugreifen

### Port-Forward einrichten

```bash
kubectl port-forward svc/timegraph 7474:7474 7687:7687 -n otel-demo
```

### Neo4j Browser öffnen

- **URL:** http://localhost:7474
- **Username:** `neo4j`
- **Passwort:** `timegraph123`

### Beispiel-Queries

```cypher
-- Alle Services anzeigen
MATCH (s:Service) RETURN s;

-- Service-Abhängigkeiten visualisieren
MATCH path = (s1:Service)-[:HAS_OPERATION]->(o1:Operation)-[:DEPENDS_ON]->(o2:Operation)<-[:HAS_OPERATION]-(s2:Service)
RETURN path;

-- Pods mit ihren Zeitbereichen
MATCH (p:Pod)
RETURN p.name, p.start, p.end, p.status
ORDER BY p.start;

-- Operations pro Service zählen
MATCH (s:Service)-[:HAS_OPERATION]->(o:Operation)
RETURN s.name AS service, count(o) AS operations
ORDER BY operations DESC;
```

## Konfiguration

### Service-URLs

Die URLs sind direkt im Manifest definiert (ohne Helm-Prefix):

```yaml
env:
  - name: PROMETHEUS_URL
    value: "http://prometheus:9090"
  - name: JAEGER_URL
    value: "http://jaeger-query:16686"
```

Diese entsprechen den Service-Namen im OTel Demo Plain-Manifest.

### Neo4j-Einstellungen

| Variable | Default | Beschreibung |
|----------|---------|--------------|
| `NEO4J_AUTH` | `neo4j/timegraph123` | Login-Credentials |
| `NEO4J_dbms_security_procedures_unrestricted` | `*` | Erlaubte Prozeduren |

### Image anpassen

Falls du das Image in einer Registry hostest:

```yaml
containers:
  - name: neo4j
    image: myregistry.io/timegraph-neo4j:v1.0  # Anpassen
    imagePullPolicy: Always                     # Oder IfNotPresent
```

### Ressourcen anpassen

```yaml
resources:
  requests:
    memory: "512Mi"
    cpu: "250m"
  limits:
    memory: "2Gi"    # Erhöhen bei großen Datenmengen
    cpu: "1000m"
```

## Unterschiede zum Helm-Manifest

| Aspekt | Helm-Variante | Plain K8s |
|--------|---------------|-----------|
| Prometheus Service | `otel-demo-prometheus-server` | `prometheus` |
| Jaeger Service | `otel-demo-jaeger-query` | `jaeger-query` |
| Namespace | konfigurierbar | `otel-demo` (fix) |
| Labels | `app: timegraph` | `app.kubernetes.io/*` |
| ConfigMap | Ja (separate) | Nein (inline env) |

## Troubleshooting

### Pod startet nicht

```bash
kubectl describe pod -l app.kubernetes.io/component=timegraph -n otel-demo
kubectl logs -l app.kubernetes.io/component=timegraph -n otel-demo --previous
```

### Prometheus/Jaeger nicht erreichbar

```bash
# Service-Namen prüfen
kubectl get svc -n otel-demo | grep -E 'prometheus|jaeger'

# Erwartete Services:
# prometheus        ClusterIP   ...   9090/TCP
# jaeger-query      ClusterIP   ...   16686/TCP
```

### DNS-Auflösung testen

```bash
kubectl run -it --rm debug --image=busybox -n otel-demo -- nslookup prometheus
kubectl run -it --rm debug --image=busybox -n otel-demo -- nslookup jaeger-query
```

### Auto-Registrierung schlägt fehl

Die Auto-Registrierung startet 30 Sekunden nach Neo4j-Start. Falls Prometheus/Jaeger noch nicht bereit sind:

```bash
# Prüfen ob Services laufen
kubectl get pods -n otel-demo | grep -E 'prometheus|jaeger'

# TimeGraph neu starten
kubectl rollout restart deployment timegraph -n otel-demo
```

### Logs komplett anzeigen

```bash
kubectl logs -l app.kubernetes.io/component=timegraph -n otel-demo --tail=100
```

## Deinstallation

### Nur TimeGraph entfernen

```bash
kubectl delete -f kubernetes/timegraph-extension.yaml
```

### Alles entfernen

```bash
kubectl delete -f opentelemetry-demo.yaml
```

## Persistente Daten (Optional)

Das Standard-Manifest verwendet `emptyDir` - Daten gehen beim Pod-Neustart verloren. Für persistente Daten:

```yaml
volumes:
  - name: data
    persistentVolumeClaim:
      claimName: timegraph-pvc

---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: timegraph-pvc
  namespace: otel-demo
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```
