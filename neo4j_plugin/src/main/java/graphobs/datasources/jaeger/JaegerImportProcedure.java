/*
 Example:
 CALL timegraph.jaeger.import_graph( "http://10.1.20.236:30212") YIELD services, operations, dependencies, durationMs, message;
 */
package graphobs.datasources.jaeger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import graphobs.matching.FieldNameResolver;
import graphobs.matching.OperationIdParser;
import graphobs.datasources.prometheus.ServicePodLinker;
import graphobs.datasources.jaeger.datastructs.JaegerServices;
import graphobs.datasources.jaeger.datastructs.JaegerTraces;
import graphobs.datasources.jaeger.datastructs.Trace;
import graphobs.datasources.jaeger.datastructs.Span;
import graphobs.datasources.jaeger.datastructs.Tag;
import graphobs.datasources.jaeger.datastructs.Process;
import graphobs.datasources.jaeger.datastructs.Reference;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class JaegerImportProcedure {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    private static final Gson gson = new Gson();
    private static final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private static final int BATCH_COMMIT_SIZE = 1000;

    @Procedure(name = "graphobs.jaeger.import_graph", mode = Mode.WRITE)
    @Description("Importiert Graph aus Jaeger. Findet API automatisch (auch unter /jaeger/ui/api). " +
            "Streamt Trace-Verarbeitung und schreibt in Batches.")
    public Stream<ImportResult> importTraces(
            @Name(value = "baseUrl", defaultValue = "http://10.1.20.236:30212") String baseUrl
    ) {
        long start = System.currentTimeMillis();

        if (baseUrl == null || baseUrl.isBlank()) {
            return Stream.of(new ImportResult(0, 0, 0, System.currentTimeMillis() - start, "baseUrl darf nicht leer sein"));
        }

        try {
            // 1. API Detection
            ApiDetectionResult detectionResult = detectApiAndFetchServices(httpClient, baseUrl);

            if (!detectionResult.success) {
                return Stream.of(new ImportResult(0, 0, 0, System.currentTimeMillis() - start,
                        "Konnte Jaeger API nicht finden. Versuchte Pfade: " + detectionResult.attemptedPaths));
            }

            String apiBase = detectionResult.apiBase;
            List<String> servicesToQuery = detectionResult.services;

            log.info("Jaeger API gefunden: %s (%d Services)", apiBase, servicesToQuery.size());

            if (servicesToQuery.isEmpty()) {
                return Stream.of(new ImportResult(0, 0, 0, System.currentTimeMillis() - start, "Keine Services gefunden via " + apiBase));
            }

            // 2. Fetch + sofortige Extraktion mit begrenzter Parallelitaet.
            //    Eine Semaphore stellt sicher, dass nie mehr als MAX_CONCURRENT_REQUESTS
            //    gleichzeitig bei Jaeger ankommen. Traces werden sofort nach Extraktion verworfen.
            Set<String> services = ConcurrentHashMap.newKeySet();
            ConcurrentHashMap<String, Map<String, Object>> operationMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, Map<String, String>> dependencyMap = new ConcurrentHashMap<>();
            Set<String> discoveredPodTagKeys = ConcurrentHashMap.newKeySet();

            long lookbackHours = 1;
            long startMicros = (System.currentTimeMillis() - (lookbackHours * 3600 * 1000)) * 1000;
            String limit = "100"; // Wenige Traces reichen fuer Struktur-Discovery

            // Sequentielles Fetching mit Retry und Pause um Jaeger nicht zu ueberlasten
            for (String svc : servicesToQuery) {
                List<Trace> traces = fetchTracesWithRetry(httpClient, apiBase, svc, startMicros, limit, 3);
                extractGraphData(traces, services, operationMap, dependencyMap, discoveredPodTagKeys);
                // Pause zwischen Services um Jaeger zu entlasten
                try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }

            if (operationMap.isEmpty() && services.isEmpty()) {
                return Stream.of(new ImportResult(0, 0, 0, System.currentTimeMillis() - start, "Keine Traces gefunden."));
            }

            List<Map<String, Object>> operations = new ArrayList<>(operationMap.values());
            List<Map<String, String>> dependencies = new ArrayList<>(dependencyMap.values());

            log.info("Extraktion abgeschlossen: %d Services, %d Operations, %d Dependencies",
                    services.size(), operations.size(), dependencies.size());

            // Determine the first discovered pod tag key (if any)
            String podTagKey = null;
            for (String candidate : FieldNameResolver.JAEGER_POD_TAG_KEYS) {
                if (discoveredPodTagKeys.contains(candidate)) {
                    podTagKey = candidate;
                    break;
                }
            }

            // 3. Batched DB Write
            writeBatched(new ArrayList<>(services), operations, dependencies, apiBase, podTagKey);

            // 4. Linker
            try {
                ServicePodLinker linker = new ServicePodLinker(db, log);
                linker.linkServicesAndPods();
            } catch (Exception e) {
                log.warn("Linker error: " + e.getMessage());
            }

            return Stream.of(new ImportResult(services.size(), operations.size(), dependencies.size(),
                    System.currentTimeMillis() - start, "Success"));

        } catch (Exception e) {
            log.error("Fehler: " + e.getMessage(), e);
            return Stream.of(new ImportResult(0, 0, 0, System.currentTimeMillis() - start, "Fehler: " + e.getMessage()));
        }
    }

    /**
     * Holt Traces mit Retry und exponentiellem Backoff.
     * Jaeger all-in-one kann bei vielen parallelen Requests ueberlasten.
     */
    private List<Trace> fetchTracesWithRetry(HttpClient client, String apiBase, String svc,
                                              long startMicros, String limit, int maxRetries) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                String tracesUrl = apiBase + "/traces?service=" + URLEncoder.encode(svc, StandardCharsets.UTF_8)
                        + "&limit=" + limit
                        + "&start=" + startMicros;

                HttpRequest tracesReq = HttpRequest.newBuilder()
                        .uri(URI.create(tracesUrl))
                        .timeout(Duration.ofSeconds(30))
                        .header("Accept", "application/json")
                        .GET()
                        .build();

                HttpResponse<String> tracesResp = client.send(tracesReq, HttpResponse.BodyHandlers.ofString());
                int tStatus = tracesResp.statusCode();
                String tBody = tracesResp.body() == null ? "" : tracesResp.body();

                if (tStatus >= 200 && tStatus < 300) {
                    JaegerTraces jt = gson.fromJson(tBody, JaegerTraces.class);
                    if (jt != null && jt.getData() != null) {
                        log.info("Traces fuer %s geholt: %d Traces", svc, jt.getData().size());
                        return jt.getData();
                    }
                    return Collections.emptyList();
                }

                // HTTP-Fehler: Bei 5xx oder 429 retry, sonst aufgeben
                if (tStatus >= 500 || tStatus == 429) {
                    log.warn("Jaeger HTTP %d fuer %s (Versuch %d/%d), warte...", tStatus, svc, attempt, maxRetries);
                } else {
                    log.warn("Jaeger HTTP %d fuer %s, kein Retry", tStatus, svc);
                    return Collections.emptyList();
                }
            } catch (JsonSyntaxException ex) {
                log.error("JSON parse error for traces of service %s: %s", svc, ex.getMessage());
                return Collections.emptyList();
            } catch (IOException | InterruptedException e) {
                log.warn("Netzwerkfehler fuer %s (Versuch %d/%d): %s", svc, attempt, maxRetries, e.getMessage());
                if (e instanceof InterruptedException) { Thread.currentThread().interrupt(); return Collections.emptyList(); }
            }
            // Exponentielles Backoff: 2s, 4s, 8s
            try { Thread.sleep(2000L * (1L << (attempt - 1))); } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); return Collections.emptyList();
            }
        }
        log.warn("Alle %d Versuche fehlgeschlagen fuer Service %s", maxRetries, svc);
        return Collections.emptyList();
    }

    /**
     * Extrahiert Services, Operations und Dependencies aus Traces.
     * Thread-safe durch ConcurrentHashMap. Traces werden nach dem Aufruf nicht mehr referenziert.
     */
    private void extractGraphData(List<Trace> traces,
                                   Set<String> services,
                                   ConcurrentHashMap<String, Map<String, Object>> operationMap,
                                   ConcurrentHashMap<String, Map<String, String>> dependencyMap,
                                   Set<String> discoveredPodTagKeys) {
        // Known pod tag key candidates for scanning
        Set<String> podTagCandidates = new HashSet<>(Arrays.asList(FieldNameResolver.JAEGER_POD_TAG_KEYS));

        for (Trace trace : traces) {
            if (trace == null || trace.getSpans() == null) continue;

            Map<String, String> spanIdToOpId = new HashMap<>();
            Map<String, Process> processes = trace.getProcesses() == null ? Collections.emptyMap() : trace.getProcesses();

            // Pass 1: Services & Operations + Pod Tag Key discovery
            for (Span sp : trace.getSpans()) {
                if (sp == null) continue;
                String procId = sp.getProcessId();
                String serviceName = "unknown-service";
                if (procId != null && processes.containsKey(procId)) {
                    Process p = processes.get(procId);
                    if (p != null && p.getServiceName() != null) {
                        serviceName = p.getServiceName();
                    }
                }

                String operationName = sp.getOperationName() == null ? "unknown-operation" : sp.getOperationName();
                String operationId = OperationIdParser.buildOperationId(serviceName, operationName);

                services.add(serviceName);

                // putIfAbsent ist thread-safe bei ConcurrentHashMap
                operationMap.putIfAbsent(operationId, Map.of(
                        "id", operationId,
                        "name", operationName,
                        "service", serviceName
                ));

                spanIdToOpId.put(sp.getSpanID(), operationId);

                // Scan span tags for pod tag keys
                scanTagsForPodKeys(sp.getTags(), podTagCandidates, discoveredPodTagKeys);

                // Scan process tags for pod tag keys
                if (procId != null && processes.containsKey(procId)) {
                    Process p = processes.get(procId);
                    if (p != null) {
                        scanTagsForPodKeys(p.getTags(), podTagCandidates, discoveredPodTagKeys);
                    }
                }
            }

            // Pass 2: Dependencies
            for (Span sp : trace.getSpans()) {
                if (sp == null || sp.getReferences() == null) continue;
                String childOpId = spanIdToOpId.get(sp.getSpanID());
                if (childOpId == null) continue;

                for (Reference ref : sp.getReferences()) {
                    if (ref == null || !"CHILD_OF".equals(ref.getRefType())) continue;
                    String parentSpanId = ref.getSpanID();
                    if (parentSpanId == null) continue;
                    String parentOpId = spanIdToOpId.get(parentSpanId);
                    if (parentOpId == null) continue;

                    String pairKey = parentOpId + "->" + childOpId;
                    dependencyMap.putIfAbsent(pairKey, Map.of(
                            "from", parentOpId,
                            "to", childOpId
                    ));
                }
            }
        }
    }

    /**
     * Scans a list of tags for known pod tag key candidates.
     */
    private static void scanTagsForPodKeys(List<Tag> tags, Set<String> candidates, Set<String> discovered) {
        if (tags == null) return;
        for (Tag tag : tags) {
            if (tag == null || tag.getKey() == null) continue;
            if (candidates.contains(tag.getKey()) && tag.getValue() != null) {
                discovered.add(tag.getKey());
            }
        }
    }

    /**
     * Schreibt Services, Operations und Dependencies in Batches in Neo4j.
     * Statt einer Riesen-Transaktion werden Chunks von BATCH_COMMIT_SIZE verwendet.
     */
    private void writeBatched(List<String> services, List<Map<String, Object>> operations,
                               List<Map<String, String>> dependencies, String apiBase, String podTagKey) {

        // Services: typischerweise wenige, koennen in einem Batch bleiben
        if (!services.isEmpty()) {
            for (int i = 0; i < services.size(); i += BATCH_COMMIT_SIZE) {
                List<String> batch = services.subList(i, Math.min(i + BATCH_COMMIT_SIZE, services.size()));
                try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
                    tx.execute("UNWIND $x AS s MERGE (n:Service {id: s}) ON CREATE SET n.name = s",
                            Map.of("x", batch));
                    tx.commit();
                }
            }
        }

        // Operations: koennen sehr viele sein
        if (!operations.isEmpty()) {
            for (int i = 0; i < operations.size(); i += BATCH_COMMIT_SIZE) {
                List<Map<String, Object>> batch = operations.subList(i, Math.min(i + BATCH_COMMIT_SIZE, operations.size()));
                try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
                    tx.execute("UNWIND $x AS o MERGE (n:Operation {id: o.id}) ON CREATE SET n.name = o.name, n.service = o.service",
                            Map.of("x", batch));
                    tx.execute("UNWIND $x AS o MATCH (s:Service {id: o.service}), (n:Operation {id: o.id}) MERGE (s)-[:HAS_OPERATION]->(n)",
                            Map.of("x", batch));
                    tx.commit();
                }
            }
            log.info("Operations geschrieben: %d in %d Batches", operations.size(),
                    (operations.size() + BATCH_COMMIT_SIZE - 1) / BATCH_COMMIT_SIZE);
        }

        // Dependencies: koennen sehr viele sein
        if (!dependencies.isEmpty()) {
            for (int i = 0; i < dependencies.size(); i += BATCH_COMMIT_SIZE) {
                List<Map<String, String>> batch = dependencies.subList(i, Math.min(i + BATCH_COMMIT_SIZE, dependencies.size()));
                try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
                    tx.execute("UNWIND $x AS d MATCH (a:Operation {id: d.from}), (b:Operation {id: d.to}) MERGE (a)-[:DEPENDS_ON]->(b)",
                            Map.of("x", batch));
                    tx.commit();
                }
            }
            log.info("Dependencies geschrieben: %d in %d Batches", dependencies.size(),
                    (dependencies.size() + BATCH_COMMIT_SIZE - 1) / BATCH_COMMIT_SIZE);
        }

        // Jaeger-Node aktualisieren (immer einzeln)
        try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
            tx.execute("MATCH (j:Jaeger) DETACH DELETE j", Collections.emptyMap());
            Map<String, Object> jaegerParams = new HashMap<>();
            jaegerParams.put("baseUrl", apiBase);
            jaegerParams.put("podTagKey", podTagKey);
            tx.execute("CREATE (j:Jaeger { baseUrl: $baseUrl })"
                    + " SET j.podTagKey = $podTagKey", jaegerParams);
            tx.commit();
        }
    }

    /**
     * Diese Methode probiert ALLE wahrscheinlichen Varianten durch, inklusive des UI-Falls.
     */
    private ApiDetectionResult detectApiAndFetchServices(HttpClient client, String inputUrl) {
        // Entferne Trailing Slash für sauberes String-Bauen
        String rawUrl = inputUrl.replaceAll("/+$", "");

        // Wir nutzen ein LinkedHashSet, um die Reihenfolge der Versuche zu steuern
        // und Duplikate zu vermeiden.
        Set<String> candidates = new LinkedHashSet<>();

        // 1. Priorität: Genau das, was der User eingegeben hat + /api
        // Wenn du http://localhost:16686/jaeger/ui eingibst, wird hier http://localhost:16686/jaeger/ui/api getestet.
        candidates.add(rawUrl + "/api");

        // 2. Priorität: Falls der User nur http://localhost:16686 eingegeben hat,
        // müssen wir raten. Wir fügen explizit deinen UI-Pfad hinzu.
        candidates.add(rawUrl + "/jaeger/ui/api"); // <--- DAS LÖST DEIN PROBLEM
        candidates.add(rawUrl + "/jaeger/api");    // Standard Ingress

        // 3. Fallback: Versuch, den Host zu extrahieren, falls der User einen tiefen Pfad
        // eingegeben hat, der aber falsch war, und wir von "root" neu suchen wollen.
        try {
            URI uri = URI.create(rawUrl);
            String root = uri.getScheme() + "://" + uri.getAuthority(); // http://localhost:16686

            // Auch hier fügen wir wieder die Kandidaten basierend auf Root hinzu
            candidates.add(root + "/api");
            candidates.add(root + "/jaeger/ui/api"); // <--- Auch hier wichtig
            candidates.add(root + "/jaeger/api");
        } catch (IllegalArgumentException e) {
            // Ignorieren, wenn URL nicht parsebar
        }

        List<String> triedPaths = new ArrayList<>();

        for (String candidateApiBase : candidates) {
            triedPaths.add(candidateApiBase);
            try {
                // Der Endpunkt zum Testen ist immer /services
                String testUrl = candidateApiBase + "/services";

                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(testUrl))
                        .timeout(Duration.ofSeconds(3)) // Fail fast
                        .header("Accept", "application/json")
                        .GET()
                        .build();

                HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());

                // Wenn wir JSON zurückbekommen und Status 200 ist, haben wir gewonnen
                if (resp.statusCode() == 200 && resp.body() != null) {
                    // Kurzer Check, ob es wirklich Jaeger JSON ist
                    JaegerServices js = gson.fromJson(resp.body(), JaegerServices.class);
                    if (js != null && js.getData() != null) {
                        return new ApiDetectionResult(true, candidateApiBase, js.getData(), triedPaths.toString());
                    }
                }
            } catch (Exception e) {
                // Fehler beim Verbinden -> Nächsten Kandidaten probieren
            }
        }

        return new ApiDetectionResult(false, null, Collections.emptyList(), triedPaths.toString());
    }

    private static class ApiDetectionResult {
        boolean success;
        String apiBase;
        List<String> services;
        String attemptedPaths;

        ApiDetectionResult(boolean success, String apiBase, List<String> services, String attemptedPaths) {
            this.success = success;
            this.apiBase = apiBase;
            this.services = services;
            this.attemptedPaths = attemptedPaths;
        }
    }

    public static class ImportResult {
        public long services;
        public long operations;
        public long dependencies;
        public long durationMs;
        public String message;

        public ImportResult(long services, long operations, long dependencies, long durationMs, String message) {
            this.services = services;
            this.operations = operations;
            this.dependencies = dependencies;
            this.durationMs = durationMs;
            this.message = message;
        }
    }
}
