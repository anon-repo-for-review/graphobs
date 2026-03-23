package graphobs.query.traces;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import org.neo4j.graphdb.*;
import graphobs.matching.FieldNameResolver;
import graphobs.matching.NodeResolver;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Stream;

public class JaegerGetSpansProcedure {

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    private static final Gson gson = new Gson();
    private static final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    // Reservierte Keys in der params-Map, die KEINE Tag-Filter sind
    private static final Set<String> CONFIG_KEYS = Set.of("range", "limit", "time", "operation", "service");

    @Procedure(name = "graphobs.data.get_spans", mode = Mode.READ)
    @Description("Fetches spans from Jaeger. Filters by operation and tags (including process tags like pod name).")
    public Stream<SpanResult> getSpans(
            @Name("service") String serviceName,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        if (serviceName == null || serviceName.isBlank()) return Stream.empty();
        Map<String, Object> safeParams = (params == null) ? Collections.emptyMap() : params;

        try {
            String baseUrl = FieldNameResolver.resolveJaegerBaseUrl(db);
            if (baseUrl == null) return Stream.empty();

            // 1. Parameter extrahieren
            String opFilter = (String) safeParams.get("operation");
            Instant endInstant = parseTime(safeParams.get("time"));
            String rangeStr = Objects.toString(safeParams.getOrDefault("range", "-1h"), "-1h");
            long rangeMillis = FieldNameResolver.parseRangeToMillis(rangeStr);
            Instant startInstant = endInstant.minusMillis(rangeMillis);

            int limit = 1000;
            if (safeParams.containsKey("limit")) {
                limit = Integer.parseInt(safeParams.get("limit").toString());
            }

            // 2. URL mit Filtern bauen
            StringBuilder urlBuilder = new StringBuilder(baseUrl)
                    .append("/traces?service=")
                    .append(URLEncoder.encode(serviceName, StandardCharsets.UTF_8));

            // DIREKT-FILTER: Operation
            if (opFilter != null && !opFilter.isBlank()) {
                urlBuilder.append("&operation=")
                        .append(URLEncoder.encode(opFilter, StandardCharsets.UTF_8));
            }

            // DIREKT-FILTER: Tags (Optional, reduziert Datenmenge enorm)
            for (Map.Entry<String, Object> entry : safeParams.entrySet()) {
                if (!CONFIG_KEYS.contains(entry.getKey())) {
                    // Jaeger API erwartet Tags im Format: &tag=key:value
                    String tagQuery = entry.getKey() + ":" + entry.getValue();
                    urlBuilder.append("&tag=")
                            .append(URLEncoder.encode(tagQuery, StandardCharsets.UTF_8));
                }
            }

            urlBuilder.append("&start=").append(startInstant.toEpochMilli() * 1000L);
            urlBuilder.append("&end=").append(endInstant.toEpochMilli() * 1000L);
            urlBuilder.append("&limit=").append(limit);

            // 3. Request absetzen
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .timeout(Duration.ofSeconds(20))
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 300) return Stream.empty();

            JaegerResponseRoot responseRoot = gson.fromJson(resp.body(), JaegerResponseRoot.class);
            if (responseRoot == null || responseRoot.data == null) return Stream.empty();

            List<SpanResult> results = new ArrayList<>();

            for (JTrace trace : responseRoot.data) {
                if (trace.spans == null) continue;

                Map<String, Map<String, Object>> processTagsMap = new HashMap<>();
                if (trace.processes != null) {
                    for (Map.Entry<String, JProcess> entry : trace.processes.entrySet()) {
                        processTagsMap.put(entry.getKey(), convertTags(entry.getValue().tags));
                    }
                }

                for (JSpan span : trace.spans) {
                    // Da Jaeger die Operation und die meisten Tags schon gefiltert hat,
                    // müssen wir hier fast nichts mehr prüfen.

                    Map<String, Object> mergedTags = new HashMap<>();
                    if (span.processID != null && processTagsMap.containsKey(span.processID)) {
                        mergedTags.putAll(processTagsMap.get(span.processID));
                    }
                    mergedTags.putAll(convertTags(span.tags));

                    // Sicherheitshalber: Falls ein Tag in den 'processes' (z.B. pod.name)
                    // steckt, den Jaeger nicht über den &tag Parameter filtern konnte:
                    if (!matchesCustomFilters(safeParams, mergedTags)) {
                        continue;
                    }

                    results.add(new SpanResult(
                            trace.traceID, span.spanID, getParentId(span),
                            span.operationName, serviceName, span.startTime,
                            span.duration, mergedTags
                    ));
                }
            }
            return results.stream();

        } catch (Exception e) {
            log.error("Error in get_spans: " + e.getMessage());
            return Stream.empty();
        }
    }


    @Procedure(name = "graphobs.data.get_spans_for", mode = Mode.READ)
    @Description("Fetches spans from Jaeger for a graph node (Service, Pod, Operation, or Server). " +
            "Resolves the service name automatically and post-filters by pod/operation if needed.")
    public Stream<SpanResult> getSpansFor(
            @Name("node") Node node,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        if (node == null) return Stream.empty();
        return fetchSpansForNodes(List.of(node), params, "get_spans_for");
    }

    @Procedure(name = "graphobs.data.get_spans_for_nodes", mode = Mode.READ)
    @Description("Fetches spans from Jaeger for a list of graph nodes (Service, Pod, Operation, or Server). " +
            "Resolves service names, queries Jaeger, and post-filters spans to match the given nodes.")
    public Stream<SpanResult> getSpansForNodes(
            @Name("nodes") List<Node> nodes,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        if (nodes == null || nodes.isEmpty()) return Stream.empty();
        return fetchSpansForNodes(nodes, params, "get_spans_for_nodes");
    }

    /**
     * Shared implementation for node-based span procedures.
     * Resolves nodes to services, fetches spans, and applies node-specific post-filters.
     *
     * If podTagKey is known and pod names are requested, injects &tag=podTagKey:podName
     * into the Jaeger query for server-side filtering. Falls back to client-side filtering
     * when podTagKey is unknown.
     */
    private Stream<SpanResult> fetchSpansForNodes(List<Node> nodes, Map<String, Object> params, String callerName) {
        NodeResolver.NodeResolution resolution = NodeResolver.resolveJaegerNodes(nodes, db);
        if (resolution.serviceNames.isEmpty()) {
            log.warn("%s: Could not resolve any services for the given nodes", callerName);
            return Stream.empty();
        }

        String storedPodTagKey = FieldNameResolver.resolveJaegerPodTagKey(db);

        boolean hasPodFilter = !resolution.podNames.isEmpty();
        boolean hasOpFilter  = !resolution.operationNames.isEmpty();
        boolean canFilterPodsServerSide = hasPodFilter && storedPodTagKey != null;
        boolean canFilterPodsClientSide = hasPodFilter && storedPodTagKey != null;

        List<SpanResult> allResults = new ArrayList<>();

        if (!resolution.passAll && canFilterPodsServerSide && !hasOpFilter
                && !resolution.podToOwnService.isEmpty()) {
            // Optimized pod path: 1 call per pod using its own owning service.
            // Avoids the N_services × N_pods cross-product — same call count as the loop variant.
            for (Map.Entry<String, String> podEntry : resolution.podToOwnService.entrySet()) {
                String podName   = podEntry.getKey();
                String ownService = podEntry.getValue();
                try {
                    Map<String, Object> enrichedParams = new HashMap<>(params == null ? Collections.emptyMap() : params);
                    enrichedParams.put(storedPodTagKey, podName);
                    getSpans(ownService, enrichedParams).forEach(allResults::add);
                } catch (Exception e) {
                    log.warn("%s: Error fetching spans for pod '%s' (service '%s'): %s",
                            callerName, podName, ownService, e.getMessage());
                }
            }
        } else if (!resolution.passAll && (canFilterPodsServerSide || hasOpFilter)) {
            // General server-side filtering for mixed operation+pod cases.
            for (String svc : resolution.serviceNames) {
                List<String> podIteration = canFilterPodsServerSide
                        ? new ArrayList<>(resolution.podNames)
                        : Collections.singletonList(null);
                List<String> opIteration = hasOpFilter
                        ? new ArrayList<>(resolution.operationNames)
                        : Collections.singletonList(null);

                for (String podName : podIteration) {
                    for (String opName : opIteration) {
                        try {
                            Map<String, Object> enrichedParams = new HashMap<>(params == null ? Collections.emptyMap() : params);
                            if (podName != null) enrichedParams.put(storedPodTagKey, podName);
                            if (opName  != null) enrichedParams.put("operation", opName);
                            getSpans(svc, enrichedParams).forEach(allResults::add);
                        } catch (Exception e) {
                            log.warn("%s: Error fetching spans for service '%s', pod '%s', op '%s': %s",
                                    callerName, svc, podName, opName, e.getMessage());
                        }
                    }
                }
            }
        } else if (!resolution.passAll) {
            // Client-side fallback: fetch all spans, then post-filter.
            if (hasPodFilter && !canFilterPodsClientSide) {
                log.info("%s: No stored podTagKey — cannot filter by pod. Returning all spans for owning service(s). " +
                         "Re-run import_jaeger to enable pod-level filtering.", callerName);
            }
            for (String svc : resolution.serviceNames) {
                try {
                    getSpans(svc, params).forEach(span -> {
                        if (spanMatchesFilter(span, resolution, storedPodTagKey, canFilterPodsClientSide)) {
                            allResults.add(span);
                        }
                    });
                } catch (Exception e) {
                    log.warn("%s: Error fetching spans for service '%s': %s", callerName, svc, e.getMessage());
                }
            }
        } else {
            // passAll (Service node): no filtering needed
            for (String svc : resolution.serviceNames) {
                try {
                    getSpans(svc, params).forEach(allResults::add);
                } catch (Exception e) {
                    log.warn("%s: Error fetching spans for service '%s': %s", callerName, svc, e.getMessage());
                }
            }
        }
        return allResults.stream();
    }


    // ----------------------------------------------------------------
    // Post-fetch span filtering
    // ----------------------------------------------------------------

    /**
     * Checks whether a span matches the filter criteria from the node resolution.
     * A span matches if it satisfies at least one criterion:
     * - its tags contain a matching pod name (only if applyPodFilter is true), OR
     * - its operationName matches a requested operation name
     *
     * @param applyPodFilter if false, pod name filtering is skipped (e.g. when podTagKey is unknown)
     */
    private boolean spanMatchesFilter(SpanResult span, NodeResolver.NodeResolution filter,
                                       String podTagKey, boolean applyPodFilter) {
        boolean hasPodFilter = applyPodFilter && !filter.podNames.isEmpty();
        boolean hasOpFilter  = !filter.operationNames.isEmpty();
        if (!hasPodFilter && !hasOpFilter) return true;

        if (hasOpFilter && span.operation != null) {
            for (String opName : filter.operationNames) {
                if (span.operation.equals(opName) || span.operation.contains(opName)) return true;
            }
        }

        if (hasPodFilter && span.tags != null) {
            if (podTagKey != null) {
                Object val = span.tags.get(podTagKey);
                if (val != null && filter.podNames.contains(val.toString())) return true;
            } else {
                for (String key : FieldNameResolver.JAEGER_POD_TAG_KEYS) {
                    Object val = span.tags.get(key);
                    if (val != null && filter.podNames.contains(val.toString())) return true;
                }
            }
        }
        return false;
    }

    /**
     * Prüft, ob alle custom keys in params auch in den tags enthalten sind und übereinstimmen.
     */
    private boolean matchesCustomFilters(Map<String, Object> params, Map<String, Object> tags) {
        for (Map.Entry<String, Object> param : params.entrySet()) {
            String key = param.getKey();
            if (CONFIG_KEYS.contains(key)) continue; // Config-Keys ignorieren

            Object requiredValue = param.getValue();
            Object actualValue = tags.get(key);

            if (actualValue == null) return false; // Tag fehlt

            // Einfacher String-Vergleich um Typ-Probleme (Int vs Long) zu vermeiden
            if (!String.valueOf(requiredValue).equals(String.valueOf(actualValue))) {
                return false;
            }
        }
        return true;
    }

    private Map<String, Object> convertTags(List<JTag> jTags) {
        Map<String, Object> map = new HashMap<>();
        if (jTags == null) return map;
        for (JTag t : jTags) {
            map.put(t.key, t.value);
        }
        return map;
    }

    private String getParentId(JSpan span) {
        if (span.references != null) {
            for (JReference ref : span.references) {
                if ("CHILD_OF".equalsIgnoreCase(ref.refType)) {
                    return ref.spanID;
                }
            }
        }
        return null;
    }

    // --- Helpers für Zeit/Config ---
    private Instant parseTime(Object t) {
        if (t == null) return Instant.now();
        if (t instanceof Number) return Instant.ofEpochMilli(((Number) t).longValue());
        try { return Instant.parse(t.toString()); }
        catch (Exception e) {
            try { return java.time.LocalDateTime.parse(t.toString()).atZone(ZoneId.systemDefault()).toInstant(); }
            catch (Exception ignored) { return Instant.now(); }
        }
    }



    // ==========================================
    // Interne DTO Klassen für Gson Deserialization
    // ==========================================
    private static class JaegerResponseRoot {
        public List<JTrace> data;
    }

    private static class JTrace {
        public String traceID;
        public List<JSpan> spans;
        public Map<String, JProcess> processes; // WICHTIG für Pod Name etc.
    }

    private static class JSpan {
        public String traceID;
        public String spanID;
        public String operationName;
        public String processID; // Link zu processes
        public long startTime; // micros
        public long duration;  // micros
        public List<JTag> tags;
        public List<JReference> references;
    }

    private static class JProcess {
        public String serviceName;
        public List<JTag> tags;
    }

    private static class JTag {
        public String key;
        public String type;
        public Object value;
    }

    private static class JReference {
        public String refType; // z.B. CHILD_OF
        public String traceID;
        public String spanID;
    }

    // ==========================================
    // Result Klasse für Neo4j Output
    // ==========================================
    public static class SpanResult {
        public String traceId;
        public String spanId;
        public String parentId;
        public String operation;
        public String service;
        public Long startTimeMicros;
        public Long durationMicros;
        public Map<String, Object> tags;

        public SpanResult(String traceId, String spanId, String parentId, String operation,
                          String service, Long startTimeMicros, Long durationMicros, Map<String, Object> tags) {
            this.traceId = traceId;
            this.spanId = spanId;
            this.parentId = parentId;
            this.operation = operation;
            this.service = service;
            this.startTimeMicros = startTimeMicros;
            this.durationMicros = durationMicros;
            this.tags = tags;
        }
    }
}