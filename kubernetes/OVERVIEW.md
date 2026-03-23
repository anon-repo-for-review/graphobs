# TimeGraph Kubernetes Integration - Übersicht

Diese Dokumentation beschreibt die Integration von TimeGraph (Neo4j mit Prometheus/Jaeger-Anbindung) in die OpenTelemetry Demo.

## Erstellte Dateien

### Root-Verzeichnis (`/timegraph/`)

| Datei | Beschreibung |
|-------|--------------|
| `Dockerfile` | Docker-Image basierend auf Neo4j 2025-community. Enthält das TimeGraph-Plugin und unterstützt Auto-Registrierung via Umgebungsvariablen. |
| `.dockerignore` | Definiert Dateien, die beim Docker-Build ignoriert werden sollen. |
| `build-docker.sh` | Build-Script das Maven-Build und Docker-Build kombiniert. |

### Kubernetes-Verzeichnis (`/timegraph/kubernetes/`)

| Datei | Beschreibung |
|-------|--------------|
| `timegraph-otel-demo.yaml` | Kubernetes-Manifest für die Verwendung mit **Helm-Installation** der OTel Demo. Service-Namen enthalten Helm-Release-Prefix. |
| `timegraph-extension.yaml` | Kubernetes-Manifest zum direkten **Anhängen an das OTel Demo Plain-Manifest**. Service-Namen ohne Prefix, Namespace `otel-demo`. |
| `install.sh` | Installationsscript für die Helm-Variante mit konfigurierbarem Release-Namen. |

### Java-Änderungen (`/timegraph/neo4j_plugin/`)

| Datei | Änderung |
|-------|----------|
| `src/main/java/core/MyNeo4jPluginExtensionFactory.java` | Neue Methode `autoRegisterDataSources()` - liest `PROMETHEUS_URL` und `JAEGER_URL` aus Umgebungsvariablen und registriert die Datenquellen automatisch beim Start. |

## Architektur

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
│                      (otel-demo namespace)                   │
│                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
│  │  Prometheus │    │   Jaeger    │    │  TimeGraph  │      │
│  │   :9090     │    │   :16686    │    │  (Neo4j)    │      │
│  └──────┬──────┘    └──────┬──────┘    │  :7474 HTTP │      │
│         │                  │           │  :7687 Bolt │      │
│         │                  │           └──────┬──────┘      │
│         │                  │                  │              │
│         └──────────────────┴──────────────────┘              │
│                     Auto-Registration                        │
│                     beim Container-Start                     │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              OpenTelemetry Demo Services             │    │
│  │  (frontend, cart, checkout, payment, etc.)           │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Welches Manifest verwenden?

| Szenario | Empfohlenes Manifest |
|----------|---------------------|
| OTel Demo via **Helm** installiert | `timegraph-otel-demo.yaml` |
| OTel Demo via **Plain YAML** installiert | `timegraph-extension.yaml` |

## Weiterführende Anleitungen

- [GUIDE-HELM.md](./GUIDE-HELM.md) - Anleitung für Helm-Installation
- [GUIDE-PLAIN-K8S.md](./GUIDE-PLAIN-K8S.md) - Anleitung für Plain Kubernetes
