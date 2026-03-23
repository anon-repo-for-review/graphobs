/*
Example 1:
CALL timegraph.data.get_trace(
  "media-service",
  { range: "-1h", limit: 100})
YIELD traceId, service, spanCount, startTime, durationMs, traceJSON
RETURN * LIMIT 50;

Example 2:
CALL timegraph.data.get_trace(
  "compose-post-service",
  {
    time: toString(datetime()),
    range: "1h",
    limit: 100,
    operation: "compose_post_server",
    status_code: 200
  }
)
YIELD traceId, service, spanCount, startTime, durationMs, traceJSON
RETURN *;

Example 3 (single node - Service, Operation, Pod, or Server):
MATCH (n:Pod {name: "media-service-abc123"})
CALL graphobs.data.get_trace_for_node(n, { range: "-1h", limit: 50 })
YIELD traceId, service, spanCount, startTime, durationMs, traceJSON
RETURN *;

Example 4 (node list):
MATCH (n:Pod) WHERE n.name STARTS WITH "media"
WITH collect(n) AS pods
CALL graphobs.data.get_traces_for_nodes(pods, { range: "-2h" })
YIELD traceId, service, spanCount, startTime, durationMs, traceJSON
RETURN *;

*/
package graphobs.query.traces;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import graphobs.datasources.jaeger.datastructs.JaegerTraces;
import graphobs.datasources.jaeger.datastructs.Span;
import graphobs.datasources.jaeger.datastructs.Tag;
import graphobs.datasources.jaeger.datastructs.Trace;
import graphobs.matching.FieldNameResolver;
import graphobs.matching.NodeResolver;
import graphobs.matching.OperationIdParser;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Stream;

public class JaegerGetTraceProcedure {

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    private static final Gson gson = new Gson();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @Procedure(name = "graphobs.data.get_trace", mode = Mode.READ)
    @Description("Fetches traces from Jaeger with filters: service, time, range, limit, operation, status_code")
    public Stream<TraceResult> getTraces(
            @Name("service") String serviceName,
            @Name("params") Map<String, Object> params
    ) {
        try {
            if (serviceName == null || serviceName.isBlank()) {
                log.warn("get_trace called with empty service name");
                return Stream.empty();
            }

            String baseUrl = FieldNameResolver.resolveJaegerBaseUrl(db);
            if (baseUrl == null || baseUrl.isBlank()) {
                log.error("No Jaeger baseUrl found in graph. Create (Jaeger {baseUrl:'...'}) first.");
                return Stream.empty();
            }

            return fetchTraces(baseUrl, serviceName, params).stream();

        } catch (Exception e) {
            log.error("Error in get_trace: %s", e.getMessage());
            return Stream.of(new TraceResult("ERROR", e.getMessage(), 0, null, 0, ""));
        }
    }

    @Procedure(name = "graphobs.data.get_traces_for_services", mode = Mode.READ)
    @Description("Fetches traces from Jaeger for a list of service names. " +
            "Queries each service and merges results.")
    public Stream<TraceResult> getTracesForServices(
            @Name("services") List<String> serviceNames,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        if (serviceNames == null || serviceNames.isEmpty()) return Stream.empty();

        String baseUrl = FieldNameResolver.resolveJaegerBaseUrl(db);
        if (baseUrl == null || baseUrl.isBlank()) {
            log.error("No Jaeger baseUrl found in graph. Create (Jaeger {baseUrl:'...'}) first.");
            return Stream.empty();
        }

        List<TraceResult> allResults = new ArrayList<>();
        Set<String> seenTraceIds = new HashSet<>();

        for (String svc : serviceNames) {
            if (svc == null || svc.isBlank()) continue;
            try {
                fetchTraces(baseUrl, svc, params).forEach(result -> {
                    // Deduplicate traces that appear in multiple services
                    if (seenTraceIds.add(result.traceId)) {
                        allResults.add(result);
                    }
                });
            } catch (Exception e) {
                log.warn("get_traces_for_services: Error fetching traces for service '%s': %s", svc, e.getMessage());
            }
        }
        return allResults.stream();
    }

    @Procedure(name = "graphobs.data.get_trace_for_node", mode = Mode.READ)
    @Description("Fetches traces from Jaeger for a graph node (Service, Operation, Pod, or Server). " +
            "Resolves the node to its owning service(s) and filters results to only include " +
            "traces that actually involve the specific node.")
    public Stream<TraceResult> getTraceForNode(
            @Name("node") Node node,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        if (node == null) return Stream.empty();
        return fetchTracesForNodes(List.of(node), params, "get_trace_for_node");
    }

    @Procedure(name = "graphobs.data.get_traces_for_nodes", mode = Mode.READ)
    @Description("Fetches traces from Jaeger for a list of graph nodes (Service, Operation, Pod, or Server). " +
            "Resolves each node to its owning service(s) and filters results to only include " +
            "traces that actually involve at least one of the given nodes.")
    public Stream<TraceResult> getTracesForNodes(
            @Name("nodes") List<Node> nodes,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        if (nodes == null || nodes.isEmpty()) return Stream.empty();
        return fetchTracesForNodes(nodes, params, "get_traces_for_nodes");
    }

    /**
     * Shared implementation for node-based trace procedures.
     * Resolves nodes to services, fetches traces, and applies node-specific post-filters.
     */
    private Stream<TraceResult> fetchTracesForNodes(List<Node> nodes, Map<String, Object> params, String callerName) {
        NodeResolver.NodeResolution resolution = NodeResolver.resolveJaegerNodes(nodes, db);
        if (resolution.serviceNames.isEmpty()) {
            log.warn("%s: Could not resolve any services for the given nodes", callerName);
            return Stream.empty();
        }

        String baseUrl = FieldNameResolver.resolveJaegerBaseUrl(db);
        if (baseUrl == null || baseUrl.isBlank()) {
            log.error("No Jaeger baseUrl found in graph. Create (Jaeger {baseUrl:'...'}) first.");
            return Stream.empty();
        }

        String storedPodTagKey = FieldNameResolver.resolveJaegerPodTagKey(db);

        boolean hasPodFilter = !resolution.podNames.isEmpty();
        boolean hasOpFilter  = !resolution.operationNames.isEmpty();
        boolean canFilterPodServerSide = hasPodFilter && storedPodTagKey != null;
        // If podTagKey unknown we cannot filter by pod — return all traces for the owning service.
        boolean canFilterPodClientSide = hasPodFilter && storedPodTagKey != null;

        List<TraceResult> allResults = new ArrayList<>();
        Set<String> seenTraceIds = new HashSet<>();

        if (!resolution.passAll && canFilterPodServerSide && !hasOpFilter
                && !resolution.podToOwnService.isEmpty()) {
            // Optimized pod path: 1 call per pod using its own owning service.
            // Avoids the N_services × N_pods cross-product — same call count as the loop variant.
            for (Map.Entry<String, String> podEntry : resolution.podToOwnService.entrySet()) {
                String podName   = podEntry.getKey();
                String ownService = podEntry.getValue();
                try {
                    Map<String, String> tagFilter = Map.of(storedPodTagKey, podName);
                    Map<String, Object> callParams = params != null ? new HashMap<>(params) : new HashMap<>();
                    fetchTracesRaw(baseUrl, ownService, callParams, tagFilter).forEach(traceWithRaw -> {
                        if (!seenTraceIds.add(traceWithRaw.trace.getTraceID())) return;
                        allResults.add(traceWithRaw.toResult(ownService));
                    });
                } catch (Exception e) {
                    log.warn("%s: Error fetching traces for pod '%s' (service '%s'): %s",
                            callerName, podName, ownService, e.getMessage());
                }
            }
        } else if (!resolution.passAll && (canFilterPodServerSide || hasOpFilter)) {
            // General server-side filtering for mixed operation+pod cases.
            List<String> operations = hasOpFilter ? new ArrayList<>(resolution.operationNames) : Collections.singletonList(null);
            List<String> pods = canFilterPodServerSide ? new ArrayList<>(resolution.podNames) : Collections.singletonList(null);

            for (String svc : resolution.serviceNames) {
                for (String opName : operations) {
                    for (String podName : pods) {
                        try {
                            Map<String, Object> enrichedParams = params != null ? new HashMap<>(params) : new HashMap<>();
                            if (opName != null) enrichedParams.put("operation", opName);
                            Map<String, String> tagFilter = (podName != null)
                                    ? Map.of(storedPodTagKey, podName)
                                    : Collections.emptyMap();
                            fetchTracesRaw(baseUrl, svc, enrichedParams, tagFilter).forEach(traceWithRaw -> {
                                if (!seenTraceIds.add(traceWithRaw.trace.getTraceID())) return;
                                allResults.add(traceWithRaw.toResult(svc));
                            });
                        } catch (Exception e) {
                            log.warn("%s: Error fetching traces for service '%s', op '%s', pod '%s': %s",
                                    callerName, svc, opName, podName, e.getMessage());
                        }
                    }
                }
            }
        } else {
            // Client-side fallback: fetch all traces, then post-filter.
            if (hasPodFilter && !canFilterPodClientSide) {
                log.info("%s: No stored podTagKey — cannot filter by pod. Returning all traces for owning service(s). " +
                         "Re-run import_jaeger to enable pod-level filtering.", callerName);
            }
            for (String svc : resolution.serviceNames) {
                try {
                    fetchTracesRaw(baseUrl, svc, params, Collections.emptyMap()).forEach(traceWithRaw -> {
                        if (!seenTraceIds.add(traceWithRaw.trace.getTraceID())) return;
                        if (resolution.passAll || traceMatchesFilter(traceWithRaw.trace, resolution, storedPodTagKey, canFilterPodClientSide)) {
                            allResults.add(traceWithRaw.toResult(svc));
                        }
                    });
                } catch (Exception e) {
                    log.warn("%s: Error fetching traces for service '%s': %s", callerName, svc, e.getMessage());
                }
            }
        }
        return allResults.stream();
    }

    // ----------------------------------------------------------------
    // Post-fetch trace filtering
    // ----------------------------------------------------------------

    /**
     * Checks whether a trace matches the filter criteria from the node resolution.
     * A trace matches if ANY span satisfies at least one criterion:
     * - span/process tags contain a matching pod name (only if applyPodFilter is true), OR
     * - span operationName matches a requested operation name
     *
     * @param applyPodFilter if false, pod name filtering is skipped (e.g. when podTagKey is unknown)
     */
    private boolean traceMatchesFilter(Trace trace, NodeResolver.NodeResolution filter,
                                        String podTagKey, boolean applyPodFilter) {
        boolean hasPodFilter = applyPodFilter && !filter.podNames.isEmpty();
        boolean hasOpFilter  = !filter.operationNames.isEmpty();
        if (!hasPodFilter && !hasOpFilter) return true;

        Map<String, graphobs.datasources.jaeger.datastructs.Process> processes = trace.getProcesses();

        for (Span span : trace.getSpans()) {
            if (span == null) continue;

            if (hasOpFilter && span.getOperationName() != null) {
                for (String opName : filter.operationNames) {
                    if (span.getOperationName().equals(opName) || span.getOperationName().contains(opName)) {
                        return true;
                    }
                }
            }

            if (hasPodFilter && matchesPodInTags(span.getTags(), filter.podNames, podTagKey)) {
                return true;
            }

            if (hasPodFilter && processes != null && span.getProcessId() != null) {
                graphobs.datasources.jaeger.datastructs.Process proc = processes.get(span.getProcessId());
                if (proc != null && matchesPodInTags(proc.getTags(), filter.podNames, podTagKey)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks if any tag in the list matches a known pod tag key with a value in the given pod name set.
     * If a stored podTagKey is available, only checks that key. Otherwise falls back to all candidates.
     */
    private static boolean matchesPodInTags(List<Tag> tags, Set<String> podNames, String storedPodTagKey) {
        if (tags == null || tags.isEmpty()) return false;
        for (Tag tag : tags) {
            if (tag == null || tag.getKey() == null || tag.getValue() == null) continue;
            if (storedPodTagKey != null) {
                // Optimized: only check the stored key
                if (storedPodTagKey.equals(tag.getKey()) && podNames.contains(tag.getValue().toString())) {
                    return true;
                }
            } else {
                // Fallback: try all known candidates
                for (String podTagKey : FieldNameResolver.JAEGER_POD_TAG_KEYS) {
                    if (podTagKey.equals(tag.getKey()) && podNames.contains(tag.getValue().toString())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // ----------------------------------------------------------------
    // Core Jaeger fetch logic
    // ----------------------------------------------------------------

    /**
     * Intermediate result holding both the raw Trace and pre-computed metadata,
     * so node-based procedures can filter before converting to TraceResult.
     */
    private static class FetchedTrace {
        final Trace trace;
        final int spanCount;
        final String startIso;
        final long durationMs;

        FetchedTrace(Trace trace, int spanCount, String startIso, long durationMs) {
            this.trace = trace;
            this.spanCount = spanCount;
            this.startIso = startIso;
            this.durationMs = durationMs;
        }

        TraceResult toResult(String serviceName) {
            try {
                String traceJSON = objectMapper.writeValueAsString(trace);
                return new TraceResult(trace.getTraceID(), serviceName, spanCount, startIso, durationMs, traceJSON);
            } catch (Exception e) {
                return new TraceResult(trace.getTraceID(), serviceName, spanCount, startIso, durationMs, "{}");
            }
        }
    }

    /**
     * Core trace-fetching logic used by string-based procedures.
     * Applies operation and status_code filters from params.
     */
    private List<TraceResult> fetchTraces(String baseUrl, String serviceName, Map<String, Object> params) throws Exception {
        List<FetchedTrace> raw = fetchTracesRaw(baseUrl, serviceName, params, Collections.emptyMap());
        List<TraceResult> results = new ArrayList<>(raw.size());
        for (FetchedTrace ft : raw) {
            results.add(ft.toResult(serviceName));
        }
        return results;
    }

    /**
     * Core trace-fetching logic returning raw Trace objects for post-filtering.
     * Applies operation and status_code filters from params.
     * @param extraTags additional tag filters to append to the Jaeger URL (e.g. podTagKey:podName)
     */
    private List<FetchedTrace> fetchTracesRaw(String baseUrl, String serviceName, Map<String, Object> params,
                                               Map<String, String> extraTags) throws Exception {
        // parse time
        Instant endInstant = Instant.now();
        if (params != null && params.containsKey("time")) {
            Object t = params.get("time");
            if (t instanceof Number) {
                endInstant = Instant.ofEpochMilli(((Number) t).longValue());
            } else {
                try {
                    endInstant = Instant.parse(t.toString());
                } catch (DateTimeParseException ex) {
                    try {
                        endInstant = java.time.LocalDateTime.parse(t.toString()).atZone(ZoneId.systemDefault()).toInstant();
                    } catch (Exception e2) {
                        log.warn("Could not parse 'time' param '%s', fallback to now: %s", t, e2.getMessage());
                        endInstant = Instant.now();
                    }
                }
            }
        }
        // parse range
        String rangeStr = params != null ? Objects.toString(params.getOrDefault("range", "-1h"), "-1h") : "-1h";
        long rangeMillis = FieldNameResolver.parseRangeToMillis(rangeStr);
        Instant startInstant = endInstant.minusMillis(rangeMillis);
        // parse limit
        int limit = 1000;
        if (params != null && params.containsKey("limit")) {
            Object limObj = params.get("limit");
            try {
                if (limObj instanceof Number) {
                    limit = ((Number) limObj).intValue();
                } else {
                    limit = Integer.parseInt(limObj.toString());
                }
            } catch (Exception e) {
                log.warn("Could not parse limit '%s', using default 1000", limObj);
            }
        }

        // parse filters
        String opFilter = params != null ? Objects.toString(params.getOrDefault("operation", null), null) : null;
        Integer statusFilter = null;
        if (params != null && params.containsKey("status_code")) {
            try {
                statusFilter = ((Number) params.get("status_code")).intValue();
            } catch (ClassCastException ice) {
                try { statusFilter = Integer.parseInt(params.get("status_code").toString()); } catch (Exception ignored) {}
            }
        }

        // Jaeger: convert millis -> micros
        long startMicros = startInstant.toEpochMilli() * 1000L;
        long endMicros = endInstant.toEpochMilli() * 1000L;

        StringBuilder urlBuilder = new StringBuilder(baseUrl)
                .append("/traces?service=").append(URLEncoder.encode(serviceName, StandardCharsets.UTF_8))
                .append("&start=").append(startMicros)
                .append("&end=").append(endMicros)
                .append("&limit=").append(limit);

        // Append server-side operation filter
        if (opFilter != null && !opFilter.isBlank()) {
            urlBuilder.append("&operation=").append(URLEncoder.encode(opFilter, StandardCharsets.UTF_8));
        }

        // Append server-side tag filters (e.g. podTagKey:podName)
        if (extraTags != null) {
            for (Map.Entry<String, String> tag : extraTags.entrySet()) {
                String tagQuery = tag.getKey() + ":" + tag.getValue();
                urlBuilder.append("&tag=").append(URLEncoder.encode(tagQuery, StandardCharsets.UTF_8));
            }
        }

        String url = urlBuilder.toString();
        log.debug("graphobs.data.get_trace - querying Jaeger: %s", url);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            log.error("Jaeger HTTP error: status=%d body=%s", resp.statusCode(), snippet(resp.body(), 500));
            return Collections.emptyList();
        }

        JaegerTraces jt = gson.fromJson(resp.body(), JaegerTraces.class);
        if (jt == null || jt.getData() == null || jt.getData().isEmpty()) {
            log.debug("No traces returned from Jaeger for service=%s in window %s - %s", serviceName, startInstant, endInstant);
            return Collections.emptyList();
        }

        List<FetchedTrace> results = new ArrayList<>();
        for (Trace t : jt.getData()) {
            if (t == null || t.getSpans() == null || t.getSpans().isEmpty()) continue;

            long minStart = Long.MAX_VALUE;
            long maxEnd = Long.MIN_VALUE;
            int spanCount = 0;

            boolean opOk = (opFilter == null);
            boolean statusOk = (statusFilter == null);

            for (Span sp : t.getSpans()) {
                if (sp == null) continue;
                spanCount++;

                long sStart = sp.getStartTime();
                long sDuration = sp.getDuration();
                long sEnd = sStart + sDuration;

                long sStartMillis = sStart / 1000L;
                long sEndMillis = sStartMillis + sDuration / 1000L;

                if (sStartMillis < minStart) minStart = sStartMillis;
                if (sEndMillis > maxEnd) maxEnd = sEndMillis;

                if (opFilter != null && sp.getOperationName() != null) {
                    if (sp.getOperationName().equals(opFilter) || sp.getOperationName().contains(opFilter)) {
                        opOk = true;
                    }
                }

                if (statusFilter != null && sp.getTags() != null) {
                    for (Tag tag : sp.getTags()) {
                        if (tag == null || tag.getKey() == null) continue;
                        String key = tag.getKey();
                        Object val = tag.getValue();
                        if ("http.status_code".equals(key) || "status.code".equals(key) || "status".equals(key)) {
                            Integer sc = tryExtractInt(val);
                            if (sc != null && sc.equals(statusFilter)) {
                                statusOk = true;
                            }
                        }
                    }
                }
            }

            if (!opOk || !statusOk) continue;

            long durationMs = 0;
            String startIso = null;
            if (minStart != Long.MAX_VALUE && maxEnd != Long.MIN_VALUE) {
                durationMs = Math.max(0, maxEnd - minStart);
                startIso = Instant.ofEpochMilli(minStart).toString();
            }
            results.add(new FetchedTrace(t, spanCount, startIso, durationMs));
        }

        if (results.isEmpty()) {
            log.debug("No traces passed client-side filters for service=%s", serviceName);
        }

        return results;
    }

    private static String snippet(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    private Integer tryExtractInt(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).intValue();
        try { return Integer.parseInt(v.toString()); } catch (Exception e) { return null; }
    }

    // Result DTO
    public static class TraceResult {
        public String traceId;
        public String service;
        public long spanCount;
        public String startTime;   // ISO string, nullable
        public long durationMs;
        public String traceJSON;

        public TraceResult(String traceId, String service, long spanCount, String startTime, long durationMs, String traceJSON) {
            this.traceId = traceId;
            this.service = service;
            this.spanCount = spanCount;
            this.startTime = startTime;
            this.durationMs = durationMs;
            this.traceJSON = traceJSON;
        }
    }
}
