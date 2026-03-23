#!/bin/bash
# Installation script for TimeGraph with OpenTelemetry Demo
#
# Prerequisites:
# - kubectl configured for your cluster
# - Docker image built and available (or pushed to registry)
# - OTel Demo already installed

set -e

NAMESPACE="${NAMESPACE:-default}"
RELEASE_NAME="${RELEASE_NAME:-otel-demo}"
IMAGE="${IMAGE:-timegraph-neo4j:latest}"

echo "=== TimeGraph OTel Demo Integration ==="
echo "Namespace: $NAMESPACE"
echo "OTel Demo Release: $RELEASE_NAME"
echo "Image: $IMAGE"
echo ""

# Check if OTel Demo is installed
if ! kubectl get deployment "${RELEASE_NAME}-prometheus-server" -n "$NAMESPACE" &> /dev/null; then
    echo "WARNING: OTel Demo Prometheus not found. Make sure OTel Demo is installed:"
    echo "  helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts"
    echo "  helm install $RELEASE_NAME open-telemetry/opentelemetry-demo -n $NAMESPACE"
    echo ""
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Update ConfigMap with correct service names based on release name
echo "Updating service URLs for release: $RELEASE_NAME"
sed -e "s/otel-demo-prometheus-server/${RELEASE_NAME}-prometheus-server/g" \
    -e "s/otel-demo-jaeger-query/${RELEASE_NAME}-jaeger-query/g" \
    timegraph-otel-demo.yaml > /tmp/timegraph-deploy.yaml

# Update image if custom
if [ "$IMAGE" != "timegraph-neo4j:latest" ]; then
    sed -i "s|image: timegraph-neo4j:latest|image: $IMAGE|g" /tmp/timegraph-deploy.yaml
fi

# Deploy
echo "Deploying TimeGraph..."
kubectl apply -f /tmp/timegraph-deploy.yaml -n "$NAMESPACE"

echo ""
echo "=== Deployment complete ==="
echo ""
echo "Wait for pod to be ready:"
echo "  kubectl get pods -l app=timegraph -n $NAMESPACE -w"
echo ""
echo "Access Neo4j Browser (port-forward):"
echo "  kubectl port-forward svc/timegraph 7474:7474 7687:7687 -n $NAMESPACE"
echo "  Open: http://localhost:7474"
echo "  Login: neo4j / timegraph123"
echo ""
echo "Check auto-registration logs:"
echo "  kubectl logs -l app=timegraph -n $NAMESPACE | grep -i 'auto-'"
