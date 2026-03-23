package graphobs.query.logs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import graphobs.matching.FieldNameResolver;
import graphobs.matching.NodeResolver;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Stream;

public class OpenSearchGetLogsProcedure {

    @Context public Log log;
    @Context public GraphDatabaseService db;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private static final Set<String> CONFIG_KEYS = Set.of("range", "limit", "time", "level", "query", "dsl");

    @Procedure(name = "graphobs.data.get_logs", mode = Mode.READ)
    @Description("Fetches logs from OpenSearch. Filters by level, query string, and field filters.")
    public Stream<LogResult> getLogs(
            @Name("index") String index,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params) {

        if (index == null || index.isBlank()) return Stream.empty();
        Map<String, Object> safeParams = (params == null) ? Collections.emptyMap() : params;

        try {
            // Load OpenSearch config from graph
            OpenSearchConfig config = loadConfigFromGraph();
            if (config == null) {
                log.error("No OpenSearch node found in graph. Register first via graphobs.datasources.register_opensearch");
                return Stream.empty();
            }

            // Parse time parameters
            Instant endInstant = parseTime(safeParams.get("time"));
            String rangeStr = Objects.toString(safeParams.getOrDefault("range", "-1h"), "-1h");
            long rangeMillis = parseRangeToMillis(rangeStr);
            Instant startInstant = endInstant.minusMillis(rangeMillis);

            int limit = 100;
            if (safeParams.containsKey("limit")) {
                limit = Integer.parseInt(safeParams.get("limit").toString());
            }

            // Build query
            JsonNode queryBody;
            if (safeParams.containsKey("dsl")) {
                // Raw DSL mode
                queryBody = MAPPER.readTree(safeParams.get("dsl").toString());
            } else {
                queryBody = buildQuery(safeParams, startInstant, endInstant, limit);
            }

            // Execute search
            String url = config.url + (config.url.endsWith("/") ? "" : "/") + index + "/_search";
            HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json");
            if (config.authHeader != null) {
                reqBuilder.header("Authorization", config.authHeader);
            }
            HttpRequest req = reqBuilder
                    .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(queryBody)))
                    .build();

            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() >= 300) {
                log.error("OpenSearch returned " + resp.statusCode() + ": " + resp.body());
                return Stream.empty();
            }

            JsonNode responseRoot = MAPPER.readTree(resp.body());
            IndexFieldMeta meta = loadIndexFieldMeta(index);
            return parseResults(responseRoot, meta).stream();

        } catch (Exception e) {
            log.error("Error in get_logs: " + e.getMessage());
            return Stream.empty();
        }
    }

    @Procedure(name = "graphobs.data.get_logs_for", mode = Mode.READ)
    @Description("Fetches logs from OpenSearch for a graph node (Service, Pod, Operation, or Server). " +
            "Server nodes are resolved to their deployed Pods via the graph.")
    public Stream<LogResult> getLogsFor(
            @Name("node") Node node,
            @Name("index") String index,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params) {

        if (node == null) return Stream.empty();

        // For Server nodes, resolve to pods and use the batch path
        Map<String, List<String>> filters = resolveNodeToFilters(node);
        if (filters.isEmpty()) {
            log.warn("get_logs_for: Node has no recognized label (Service, Pod, Operation, Server)");
            return Stream.empty();
        }

        // If single filter value, use the simple path
        if (filters.size() == 1) {
            Map.Entry<String, List<String>> entry = filters.entrySet().iterator().next();
            if (entry.getValue().size() == 1) {
                Map<String, Object> mergedParams = new HashMap<>(params == null ? Collections.emptyMap() : params);
                mergedParams.put(entry.getKey(), entry.getValue().get(0));
                return getLogs(index, mergedParams);
            }
        }

        // Multiple filter values (e.g. Server with multiple Pods) → use batch query
        return getLogsForNodes(List.of(node), index, params);
    }

    @Procedure(name = "graphobs.data.get_logs_for_nodes", mode = Mode.READ)
    @Description("Fetches logs from OpenSearch for a list of graph nodes (Service, Pod, Operation, or Server). " +
            "Server nodes are resolved to their deployed Pods. Builds a single OpenSearch query with OR-filter for all nodes.")
    public Stream<LogResult> getLogsForNodes(
            @Name("nodes") List<Node> nodes,
            @Name("index") String index,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params) {

        if (nodes == null || nodes.isEmpty()) return Stream.empty();
        if (index == null || index.isBlank()) return Stream.empty();

        Map<String, Object> safeParams = (params == null) ? Collections.emptyMap() : params;

        try {
            OpenSearchConfig config = loadConfigFromGraph();
            if (config == null) {
                log.error("No OpenSearch node found in graph. Register first via graphobs.datasources.register_opensearch");
                return Stream.empty();
            }

            // Resolve all nodes to OpenSearch filters, grouped by field
            // Server nodes are expanded to their Pods via graph lookup
            Map<String, List<String>> fieldToValues = new LinkedHashMap<>();
            for (Node node : nodes) {
                if (node == null) continue;
                Map<String, List<String>> nodeFilters = resolveNodeToFilters(node);
                for (Map.Entry<String, List<String>> entry : nodeFilters.entrySet()) {
                    fieldToValues.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).addAll(entry.getValue());
                }
            }

            if (fieldToValues.isEmpty()) {
                log.warn("get_logs_for_nodes: No nodes could be resolved to OpenSearch filters.");
                return Stream.empty();
            }

            // Parse time parameters
            Instant endInstant = parseTime(safeParams.get("time"));
            String rangeStr = Objects.toString(safeParams.getOrDefault("range", "-1h"), "-1h");
            long rangeMillis = parseRangeToMillis(rangeStr);
            Instant startInstant = endInstant.minusMillis(rangeMillis);

            int limit = 100;
            if (safeParams.containsKey("limit")) {
                limit = Integer.parseInt(safeParams.get("limit").toString());
            }

            // Build query with bool.should for all node filters
            JsonNode queryBody = buildBatchQuery(safeParams, startInstant, endInstant, limit, fieldToValues);

            // Execute search
            String url = config.url + (config.url.endsWith("/") ? "" : "/") + index + "/_search";
            HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json");
            if (config.authHeader != null) {
                reqBuilder.header("Authorization", config.authHeader);
            }
            HttpRequest req = reqBuilder
                    .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(queryBody)))
                    .build();

            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() >= 300) {
                log.error("OpenSearch returned " + resp.statusCode() + ": " + resp.body());
                return Stream.empty();
            }

            JsonNode responseRoot = MAPPER.readTree(resp.body());
            IndexFieldMeta meta = loadIndexFieldMeta(index);
            return parseResults(responseRoot, meta).stream();

        } catch (Exception e) {
            log.error("Error in get_logs_for_nodes: " + e.getMessage());
            return Stream.empty();
        }
    }

    /**
     * Resolves a node to OpenSearch filter(s).
     * For Service/Pod/Operation: delegates to FieldNameResolver.
     * For Server: resolves to deployed Pod names via NodeResolver.
     */
    private Map<String, List<String>> resolveNodeToFilters(Node node) {
        Map<String, List<String>> result = new LinkedHashMap<>();
        if (node == null) return result;

        // Try standard resolution first (Service, Pod, Operation)
        String[] filter = FieldNameResolver.resolveNodeToOpenSearchFilter(node);
        if (filter != null) {
            result.computeIfAbsent(filter[0], k -> new ArrayList<>()).add(filter[1]);
            return result;
        }

        // Server node: resolve to Pods via NodeResolver
        if (node.hasLabel(Label.label("Server"))) {
            List<String> podNames = NodeResolver.resolveServerToPodNames(node, db);
            if (!podNames.isEmpty()) {
                result.put(FieldNameResolver.OPENSEARCH_POD_QUERY_FIELD, podNames);
            } else {
                log.warn("resolveNodeToFilters: Server node %s has no deployed Pods", node.getElementId());
            }
        }

        return result;
    }

    private JsonNode buildBatchQuery(Map<String, Object> params, Instant start, Instant end, int limit,
                                     Map<String, List<String>> fieldToValues) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("size", limit);

        ArrayNode sortArr = root.putArray("sort");
        ObjectNode sortEntry = sortArr.addObject();
        ObjectNode tsSort = sortEntry.putObject("@timestamp");
        tsSort.put("order", "desc");

        ObjectNode query = root.putObject("query");
        ObjectNode bool = query.putObject("bool");
        ArrayNode must = bool.putArray("must");

        // Time range filter
        ObjectNode rangeClause = must.addObject();
        ObjectNode rangeObj = rangeClause.putObject("range");
        ObjectNode tsRange = rangeObj.putObject("@timestamp");
        tsRange.put("gte", start.toString());
        tsRange.put("lte", end.toString());

        // Node filter: bool.should with terms for all nodes
        ObjectNode nodeFilterClause = must.addObject();
        ObjectNode nodeFilterBool = nodeFilterClause.putObject("bool");
        ArrayNode should = nodeFilterBool.putArray("should");
        for (Map.Entry<String, List<String>> entry : fieldToValues.entrySet()) {
            String field = entry.getKey();
            List<String> values = entry.getValue();
            if (values.size() == 1) {
                ObjectNode termClause = should.addObject();
                ObjectNode term = termClause.putObject("term");
                term.put(field, values.get(0));
            } else {
                ObjectNode termsClause = should.addObject();
                ObjectNode terms = termsClause.putObject("terms");
                ArrayNode arr = terms.putArray(field);
                for (String v : values) arr.add(v);
            }
        }
        nodeFilterBool.put("minimum_should_match", 1);

        // Level filter
        if (params.containsKey("level")) {
            String levelVal = params.get("level").toString().toLowerCase();
            ObjectNode levelClause = must.addObject();
            ObjectNode bool2 = levelClause.putObject("bool");
            ArrayNode levelShould = bool2.putArray("should");
            ObjectNode opt1 = levelShould.addObject();
            ObjectNode match1 = opt1.putObject("match");
            ObjectNode match1Field = match1.putObject("severity.text");
            match1Field.put("query", levelVal);
            match1Field.put("operator", "or");
            ObjectNode opt2 = levelShould.addObject();
            ObjectNode match2 = opt2.putObject("match");
            ObjectNode match2Field = match2.putObject("severity_text");
            match2Field.put("query", levelVal);
            match2Field.put("operator", "or");
            bool2.put("minimum_should_match", 1);
        }

        // Query string
        if (params.containsKey("query")) {
            ObjectNode qsClause = must.addObject();
            ObjectNode queryString = qsClause.putObject("query_string");
            queryString.put("query", params.get("query").toString());
        }

        // Custom field filters
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (!CONFIG_KEYS.contains(entry.getKey())) {
                ObjectNode termClause = must.addObject();
                ObjectNode term = termClause.putObject("term");
                String fieldName = entry.getKey();
                if (!fieldName.endsWith(".keyword")) {
                    fieldName = fieldName + ".keyword";
                }
                term.put(fieldName, entry.getValue().toString());
            }
        }

        return root;
    }

    private JsonNode buildQuery(Map<String, Object> params, Instant start, Instant end, int limit) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("size", limit);

        // Sort by timestamp descending
        ArrayNode sortArr = root.putArray("sort");
        ObjectNode sortEntry = sortArr.addObject();
        ObjectNode tsSort = sortEntry.putObject("@timestamp");
        tsSort.put("order", "desc");

        ObjectNode query = root.putObject("query");
        ObjectNode bool = query.putObject("bool");
        ArrayNode must = bool.putArray("must");

        // Time range filter
        ObjectNode rangeClause = must.addObject();
        ObjectNode rangeObj = rangeClause.putObject("range");
        ObjectNode tsRange = rangeObj.putObject("@timestamp");
        tsRange.put("gte", start.toString());
        tsRange.put("lte", end.toString());

        // Level filter (case-insensitive, OTel format: severity.text)
        if (params.containsKey("level")) {
            String levelVal = params.get("level").toString().toLowerCase();
            ObjectNode levelClause = must.addObject();
            ObjectNode bool2 = levelClause.putObject("bool");
            ArrayNode should = bool2.putArray("should");
            // OTel nested format: severity.text (case-insensitive match)
            ObjectNode opt1 = should.addObject();
            ObjectNode match1 = opt1.putObject("match");
            ObjectNode match1Field = match1.putObject("severity.text");
            match1Field.put("query", levelVal);
            match1Field.put("operator", "or");
            // Flat format fallback: severity_text
            ObjectNode opt2 = should.addObject();
            ObjectNode match2 = opt2.putObject("match");
            ObjectNode match2Field = match2.putObject("severity_text");
            match2Field.put("query", levelVal);
            match2Field.put("operator", "or");
            bool2.put("minimum_should_match", 1);
        }

        // Query string (full-text search)
        if (params.containsKey("query")) {
            ObjectNode qsClause = must.addObject();
            ObjectNode queryString = qsClause.putObject("query_string");
            queryString.put("query", params.get("query").toString());
        }

        // Custom field filters (all non-config keys)
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (!CONFIG_KEYS.contains(entry.getKey())) {
                ObjectNode termClause = must.addObject();
                ObjectNode term = termClause.putObject("term");
                String fieldName = entry.getKey();
                // Append .keyword for exact match, unless already present
                if (!fieldName.endsWith(".keyword")) {
                    fieldName = fieldName + ".keyword";
                }
                term.put(fieldName, entry.getValue().toString());
            }
        }

        return root;
    }

    private List<LogResult> parseResults(JsonNode responseRoot, IndexFieldMeta meta) {
        List<LogResult> results = new ArrayList<>();
        if (responseRoot == null) return results;

        JsonNode hits = responseRoot.path("hits").path("hits");
        if (!hits.isArray()) return results;

        for (JsonNode hit : hits) {
            JsonNode source = hit.path("_source");
            if (source.isMissingNode()) continue;

            String timestamp = source.path("@timestamp").asText(null);
            String level = extractLevel(source, meta);
            String message = extractMessage(source, meta);

            // Convert source to Map for field resolution
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = MAPPER.convertValue(source, Map.class);
            String sourceName = resolveSource(fields);

            results.add(new LogResult(timestamp, level, message, sourceName, fields));
        }
        return results;
    }

    private String extractLevel(JsonNode source, IndexFieldMeta meta) {
        // If stored severity field is known, try it first
        if (meta.severityField != null) {
            String val = getNestedJsonText(source, meta.severityField);
            if (val != null) return val;
        }
        // OTel format: severity is an object with "text" and "number"
        JsonNode sevText = source.path("severity").path("text");
        if (!sevText.isMissingNode() && sevText.isTextual() && !sevText.asText().isEmpty()) {
            return sevText.asText();
        }
        // Fallback: try flat field names
        for (String field : new String[]{"severity_text", "level", "log.level"}) {
            String val = getNestedJsonText(source, field);
            if (val != null) return val;
        }
        return null;
    }

    private String extractMessage(JsonNode source, IndexFieldMeta meta) {
        // If stored message field is known, try it first
        if (meta.messageField != null) {
            String val = getNestedJsonText(source, meta.messageField);
            if (val != null) return val;
        }
        // Fallback: try all candidates
        for (String field : new String[]{"body", "message", "msg", "log"}) {
            String val = getNestedJsonText(source, field);
            if (val != null) return val;
        }
        return null;
    }

    private String getNestedJsonText(JsonNode node, String dottedKey) {
        String[] parts = dottedKey.split("\\.");
        JsonNode current = node;
        for (String part : parts) {
            current = current.path(part);
            if (current.isMissingNode()) return null;
        }
        return current.isTextual() ? current.asText() : null;
    }

    @SuppressWarnings("unchecked")
    private String resolveSource(Map<String, Object> fields) {
        String svc = FieldNameResolver.resolveFirst(fields, FieldNameResolver.OPENSEARCH_SERVICE_FIELDS);
        if (svc != null) return svc;
        String pod = FieldNameResolver.resolveFirst(fields, FieldNameResolver.OPENSEARCH_POD_FIELDS);
        if (pod != null) return pod;
        return null;
    }

    private OpenSearchConfig loadConfigFromGraph() {
        try (var tx = db.beginTx()) {
            var res = tx.execute("MATCH (os:OpenSearch) RETURN os.url AS url, os.user AS user, os.password AS password LIMIT 1");
            if (res.hasNext()) {
                Map<String, Object> row = res.next();
                String url = Objects.toString(row.get("url"), null);
                if (url == null) return null;
                String user = Objects.toString(row.get("user"), null);
                String password = Objects.toString(row.get("password"), null);
                return new OpenSearchConfig(url, user, password);
            }
        }
        return null;
    }

    // --- Time helpers (same pattern as JaegerGetSpansProcedure) ---
    private Instant parseTime(Object t) {
        if (t == null) return Instant.now();
        if (t instanceof Number) return Instant.ofEpochMilli(((Number) t).longValue());
        try { return Instant.parse(t.toString()); }
        catch (Exception e) {
            try { return java.time.LocalDateTime.parse(t.toString()).atZone(ZoneId.systemDefault()).toInstant(); }
            catch (Exception ignored) { return Instant.now(); }
        }
    }

    private long parseRangeToMillis(String range) {
        if (range == null || range.isBlank()) return 3600_000L;
        String s = range.trim().replace("-", "");
        long mult = 1000L;
        if (s.endsWith("ms")) { mult = 1L; s = s.substring(0, s.length()-2); }
        else if (s.endsWith("h")) { mult = 3600000L; s = s.substring(0, s.length()-1); }
        else if (s.endsWith("m")) { mult = 60000L; s = s.substring(0, s.length()-1); }
        else if (s.endsWith("s")) { mult = 1000L; s = s.substring(0, s.length()-1); }
        else if (s.endsWith("d")) { mult = 86400000L; s = s.substring(0, s.length()-1); }
        try { return Long.parseLong(s) * mult; } catch (Exception e) { return 3600_000L; }
    }

    /**
     * Holds resolved field names for a specific LogIndex node.
     * Fields may be null if not stored (old graph without metadata).
     */
    private static class IndexFieldMeta {
        final String podField;
        final String serviceField;
        final String severityField;
        final String messageField;

        IndexFieldMeta(String podField, String serviceField, String severityField, String messageField) {
            this.podField = podField;
            this.serviceField = serviceField;
            this.severityField = severityField;
            this.messageField = messageField;
        }

        static final IndexFieldMeta EMPTY = new IndexFieldMeta(null, null, null, null);
    }

    private IndexFieldMeta loadIndexFieldMeta(String indexName) {
        if (indexName == null || indexName.isBlank()) return IndexFieldMeta.EMPTY;
        try (var tx = db.beginTx()) {
            var res = tx.execute(
                    "MATCH (idx:LogIndex {name:$name}) RETURN idx.podField AS podField, idx.serviceField AS serviceField, " +
                    "idx.severityField AS severityField, idx.messageField AS messageField",
                    Collections.singletonMap("name", indexName));
            if (res.hasNext()) {
                Map<String, Object> row = res.next();
                return new IndexFieldMeta(
                        row.get("podField") != null ? row.get("podField").toString() : null,
                        row.get("serviceField") != null ? row.get("serviceField").toString() : null,
                        row.get("severityField") != null ? row.get("severityField").toString() : null,
                        row.get("messageField") != null ? row.get("messageField").toString() : null
                );
            }
        }
        return IndexFieldMeta.EMPTY;
    }

    // --- Inner classes ---
    private static class OpenSearchConfig {
        final String url;
        final String authHeader;

        OpenSearchConfig(String url, String user, String password) {
            this.url = url;
            if (user != null && !user.isBlank()) {
                String credentials = user + ":" + (password != null ? password : "");
                this.authHeader = "Basic " + Base64.getEncoder().encodeToString(
                        credentials.getBytes(StandardCharsets.UTF_8));
            } else {
                this.authHeader = null;
            }
        }
    }

    public static class LogResult {
        public String timestamp;
        public String level;
        public String message;
        public String source;
        public Map<String, Object> fields;

        public LogResult(String timestamp, String level, String message, String source, Map<String, Object> fields) {
            this.timestamp = timestamp;
            this.level = level;
            this.message = message;
            this.source = source;
            this.fields = fields;
        }
    }
}
