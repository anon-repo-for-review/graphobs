package graphobs.matching;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

import java.util.Collections;
import java.util.Map;

public class FieldNameResolver {

    /** Prometheus label candidates for pod identification (tried in order). */
    public static final String[] PROMETHEUS_POD_LABELS = {
            "pod", "pod_name", "kubernetes_pod_name",
            "container", "container_name", "name",
            "container_label_io_kubernetes_pod_name",
            "exported_pod", "pod_id", "com_docker_compose_service"
    };

    /** Jaeger span/process tag keys that may contain the pod name. */
    public static final String[] JAEGER_POD_TAG_KEYS = {
            "k8s.pod.name", "pod.name", "pod_name", "kubernetes.pod.name"
    };

    /** OpenSearch document fields that may contain the pod name. */
    public static final String[] OPENSEARCH_POD_FIELDS = {
            "resource.k8s.pod.name", "kubernetes.pod.name", "kubernetes.pod_name", "pod.name", "container.name"
    };

    /** OpenSearch document fields that may contain the service name. */
    public static final String[] OPENSEARCH_SERVICE_FIELDS = {
            "resource.service.name", "service.name", "kubernetes.labels.app", "application"
    };

    /** OpenSearch query field for service name filtering. */
    public static final String OPENSEARCH_SERVICE_QUERY_FIELD = "resource.service.name.keyword";

    /** OpenSearch query field for pod name filtering. */
    public static final String OPENSEARCH_POD_QUERY_FIELD = "resource.k8s.pod.name.keyword";

    /**
     * Resolves a Neo4j node (Service, Pod, Operation) to an OpenSearch filter.
     * Returns a two-element array: [opensearch_field, value], or null if unknown.
     * Uses hardcoded default fields. For index-aware resolution, use the overload with db and indexName.
     */
    public static String[] resolveNodeToOpenSearchFilter(Node node) {
        return resolveNodeToOpenSearchFilter(node, null, null);
    }

    /**
     * Resolves a Neo4j node to an OpenSearch filter, optionally using stored field mappings from the LogIndex node.
     * If indexName and db are provided, loads stored field names from the graph. Falls back to hardcoded defaults.
     */
    public static String[] resolveNodeToOpenSearchFilter(Node node, GraphDatabaseService db, String indexName) {
        if (node == null) return null;

        // Try to load stored fields from LogIndex if available
        String storedPodField = null;
        String storedServiceField = null;
        if (db != null && indexName != null && !indexName.isBlank()) {
            Map<String, String> stored = loadLogIndexFields(db, indexName);
            storedPodField = stored.get("podField");
            storedServiceField = stored.get("serviceField");
        }

        if (node.hasLabel(Label.label("Service"))) {
            String id = (String) node.getProperty("id", null);
            if (id != null) {
                String field = storedServiceField != null ? storedServiceField + ".keyword" : OPENSEARCH_SERVICE_QUERY_FIELD;
                return new String[]{field, id};
            }
        }
        if (node.hasLabel(Label.label("Pod"))) {
            String name = (String) node.getProperty("name", null);
            if (name != null) {
                String field = storedPodField != null ? storedPodField + ".keyword" : OPENSEARCH_POD_QUERY_FIELD;
                return new String[]{field, name};
            }
        }
        if (node.hasLabel(Label.label("Operation"))) {
            String opId = (String) node.getProperty("id", null);
            String svc = OperationIdParser.extractServiceName(opId);
            if (svc != null) {
                String field = storedServiceField != null ? storedServiceField + ".keyword" : OPENSEARCH_SERVICE_QUERY_FIELD;
                return new String[]{field, svc};
            }
        }
        return null;
    }

    /**
     * Loads stored field mappings from a LogIndex node.
     */
    private static Map<String, String> loadLogIndexFields(GraphDatabaseService db, String indexName) {
        try (var tx = db.beginTx()) {
            Result r = tx.execute(
                    "MATCH (idx:LogIndex {name:$name}) RETURN idx.podField AS podField, idx.serviceField AS serviceField, " +
                    "idx.severityField AS severityField, idx.messageField AS messageField",
                    Collections.singletonMap("name", indexName));
            if (r.hasNext()) {
                Map<String, Object> row = r.next();
                java.util.HashMap<String, String> result = new java.util.HashMap<>();
                if (row.get("podField") != null) result.put("podField", row.get("podField").toString());
                if (row.get("serviceField") != null) result.put("serviceField", row.get("serviceField").toString());
                if (row.get("severityField") != null) result.put("severityField", row.get("severityField").toString());
                if (row.get("messageField") != null) result.put("messageField", row.get("messageField").toString());
                return result;
            }
        }
        return Collections.emptyMap();
    }

    // --- Centralized resolution methods ---

    /**
     * Resolves the Prometheus label for a given entity type from the Prometheus node.
     * @param prometheusNode the :Prometheus node
     * @param entityType "pod" or "server"
     * @return the stored label name, or null if not available (triggering fallback behavior)
     */
    public static String resolvePrometheusLabel(Node prometheusNode, String entityType) {
        if (prometheusNode == null || entityType == null) return null;
        switch (entityType) {
            case "pod": return (String) prometheusNode.getProperty("podLabel", null);
            case "server": return (String) prometheusNode.getProperty("serverLabel", null);
            default: return null;
        }
    }

    /**
     * Resolves the Jaeger base API URL from the :Jaeger node in the graph.
     * @return the stored base URL (e.g. "http://jaeger-query:16686/api"), or null if not found
     */
    public static String resolveJaegerBaseUrl(GraphDatabaseService db) {
        try (var tx = db.beginTx()) {
            Result r = tx.execute("MATCH (j:Jaeger) RETURN j.baseUrl AS baseUrl LIMIT 1");
            if (r.hasNext()) {
                Object val = r.next().get("baseUrl");
                return val != null ? val.toString() : null;
            }
        }
        return null;
    }

    /**
     * Resolves the Jaeger pod tag key from the :Jaeger node in the graph.
     * @return the stored pod tag key, or null if not available (triggering fallback behavior)
     */
    public static String resolveJaegerPodTagKey(GraphDatabaseService db) {
        try (var tx = db.beginTx()) {
            Result r = tx.execute("MATCH (j:Jaeger) RETURN j.podTagKey AS podTagKey LIMIT 1");
            if (r.hasNext()) {
                Object val = r.next().get("podTagKey");
                return val != null ? val.toString() : null;
            }
        }
        return null;
    }

    /**
     * Parses a range string (e.g. "-1h", "30m", "45s", "500ms") to milliseconds.
     * Supports negative values (leading "-" is stripped).
     * Defaults to 1 hour if unparseable.
     */
    public static long parseRangeToMillis(String range) {
        if (range == null || range.isBlank()) return 3_600_000L;
        String s = range.trim();
        if (s.startsWith("-")) s = s.substring(1);
        try {
            if (s.endsWith("ms")) return Long.parseLong(s.substring(0, s.length() - 2));
            if (s.endsWith("h"))  return Long.parseLong(s.substring(0, s.length() - 1)) * 3_600_000L;
            if (s.endsWith("m"))  return Long.parseLong(s.substring(0, s.length() - 1)) * 60_000L;
            if (s.endsWith("s"))  return Long.parseLong(s.substring(0, s.length() - 1)) * 1_000L;
            // plain number → treat as minutes
            return Long.parseLong(s) * 60_000L;
        } catch (Exception ex) {
            return 3_600_000L;
        }
    }

    /**
     * Resolves an OpenSearch field for a given entity type from the :LogIndex node.
     * @param indexName the index name
     * @param entityType "pod", "service", "severity", or "message"
     * @param db the graph database service
     * @return the stored field name, or null if not available (triggering fallback behavior)
     */
    public static String resolveOpenSearchField(String indexName, String entityType, GraphDatabaseService db) {
        if (indexName == null || entityType == null || db == null) return null;
        Map<String, String> fields = loadLogIndexFields(db, indexName);
        switch (entityType) {
            case "pod": return fields.get("podField");
            case "service": return fields.get("serviceField");
            case "severity": return fields.get("severityField");
            case "message": return fields.get("messageField");
            default: return null;
        }
    }

    /**
     * Returns the first non-null, non-empty value found in the map
     * for the given candidate keys (tried in order).
     */
    public static String resolveFirst(Map<String, Object> fields, String[] candidates) {
        if (fields == null || candidates == null) return null;
        for (String key : candidates) {
            Object val = getNestedField(fields, key);
            if (val != null) {
                String s = val.toString().trim();
                if (!s.isEmpty()) return s;
            }
        }
        return null;
    }

    /**
     * Retrieves a value from a map, supporting dot-notation for nested maps.
     * Handles mixed nesting: e.g. map["resource"]["service.name"] is found
     * via key "resource.service.name" by trying all possible split points.
     */
    @SuppressWarnings("unchecked")
    private static Object getNestedField(Map<String, Object> map, String dottedKey) {
        // First try direct lookup (covers flat maps and keys with literal dots)
        if (map.containsKey(dottedKey)) {
            return map.get(dottedKey);
        }
        // Try all possible split points: split at each dot position
        // This handles cases like map["resource"]["service.name"]
        // where the key is "resource.service.name"
        for (int i = 0; i < dottedKey.length(); i++) {
            if (dottedKey.charAt(i) == '.') {
                String head = dottedKey.substring(0, i);
                String tail = dottedKey.substring(i + 1);
                Object child = map.get(head);
                if (child instanceof Map) {
                    Object result = getNestedField((Map<String, Object>) child, tail);
                    if (result != null) return result;
                }
            }
        }
        return null;
    }
}
