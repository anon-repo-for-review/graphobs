# TimeGraph Integration - Helm Installation

Diese Anleitung beschreibt die Integration von TimeGraph in eine **Helm-basierte** OpenTelemetry Demo Installation.

**Verwendetes Manifest:** `timegraph-otel-demo.yaml`

## Voraussetzungen

- Docker installiert
- kubectl konfiguriert für deinen Cluster
- Helm installiert
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

## Schritt 2: OpenTelemetry Demo installieren

```bash
# Helm Repository hinzufügen
helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts
helm repo update

# OTel Demo installieren (Standard Release-Name: otel-demo)
helm install otel-demo open-telemetry/opentelemetry-demo

# Warten bis alle Pods laufen
kubectl get pods -w
```

## Schritt 3: TimeGraph deployen

### Option A: Mit Install-Script

```bash
cd kubernetes

# Standard-Installation (Release-Name: otel-demo, Namespace: default)
./install.sh

# Mit angepassten Werten
NAMESPACE=monitoring \
RELEASE_NAME=my-otel \
IMAGE=myregistry/timegraph-neo4j:v1.0 \
./install.sh
```

### Option B: Manuell

```bash
# Manifest anwenden
kubectl apply -f timegraph-otel-demo.yaml

# Bei anderem Release-Namen vorher anpassen:
sed -e 's/otel-demo-prometheus-server/MY-RELEASE-prometheus-server/g' \
    -e 's/otel-demo-jaeger-query/MY-RELEASE-jaeger-query/g' \
    timegraph-otel-demo.yaml | kubectl apply -f -
```

## Schritt 4: Installation verifizieren

### Pod-Status prüfen

```bash
kubectl get pods -l app=timegraph
```

Erwartete Ausgabe:
```
NAME                         READY   STATUS    RESTARTS   AGE
timegraph-xxxxxxxxx-xxxxx    1/1     Running   0          2m
```

### Logs prüfen (Auto-Registrierung)

```bash
kubectl logs -l app=timegraph | grep -i "auto-"
```

Erwartete Log-Einträge nach ~30 Sekunden:
```
Auto-registering Prometheus data source: http://otel-demo-prometheus-server:9090
Prometheus registration complete. Registered X pods.
Auto-importing Jaeger graph: http://otel-demo-jaeger-query:16686
Jaeger import result: Success (services=X, operations=X, dependencies=X)
```

## Schritt 5: Auf Neo4j zugreifen

### Port-Forward einrichten

```bash
kubectl port-forward svc/timegraph 7474:7474 7687:7687
```

### Neo4j Browser öffnen

- **URL:** http://localhost:7474
- **Username:** `neo4j`
- **Passwort:** `timegraph123`

### Beispiel-Queries

```cypher
-- Alle Services anzeigen
MATCH (s:Service) RETURN s;

-- Service-Abhängigkeiten
MATCH (s1:Service)-[:HAS_OPERATION]->(o1:Operation)-[:DEPENDS_ON]->(o2:Operation)<-[:HAS_OPERATION]-(s2:Service)
RETURN DISTINCT s1.name, s2.name;

-- Pods von Prometheus
MATCH (p:Pod) RETURN p.name, p.status;
```

## Konfiguration

### Umgebungsvariablen

Die Service-URLs werden über eine ConfigMap konfiguriert:

```yaml
# In timegraph-otel-demo.yaml
data:
  PROMETHEUS_URL: "http://otel-demo-prometheus-server:9090"
  JAEGER_URL: "http://otel-demo-jaeger-query:16686"
```

### Neo4j-Einstellungen

| Variable | Default | Beschreibung |
|----------|---------|--------------|
| `NEO4J_AUTH` | `neo4j/timegraph123` | Login-Credentials |
| `NEO4J_dbms_security_procedures_unrestricted` | `*` | Erlaubte Prozeduren |

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

## Troubleshooting

### Pod startet nicht

```bash
kubectl describe pod -l app=timegraph
kubectl logs -l app=timegraph --previous
```

### Prometheus/Jaeger nicht erreichbar

```bash
# Service-Namen prüfen
kubectl get svc | grep -E 'prometheus|jaeger'

# DNS-Auflösung testen
kubectl run -it --rm debug --image=busybox -- nslookup otel-demo-prometheus-server
```

### Auto-Registrierung schlägt fehl

Prüfe ob die Services laufen:
```bash
kubectl get pods | grep -E 'prometheus|jaeger'
```

Die Auto-Registrierung startet 30 Sekunden nach Neo4j-Start. Falls Prometheus/Jaeger noch nicht bereit sind, starte den Pod neu:
```bash
kubectl rollout restart deployment timegraph
```

## Deinstallation

```bash
kubectl delete -f timegraph-otel-demo.yaml
```
