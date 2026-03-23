package graphobs.datasources.opensearch;

import org.neo4j.logging.Log;

import java.util.*;
import java.util.concurrent.*;

public final class OpenSearchToNeo4jUpdater {
    private static final ConcurrentMap<String, ScheduledExecutorService> SCHED = new ConcurrentHashMap<>();

    private final OpenSearchClient client;
    private final LogRepository repo;
    private final Log log;
    private final String opensearchUrl;
    private final String indexPattern;
    private final long intervalSeconds;

    public OpenSearchToNeo4jUpdater(OpenSearchClient client, LogRepository repo, Log log,
                                     String url, String indexPattern, long intervalSeconds) {
        this.client = client;
        this.repo = repo;
        this.log = log;
        this.opensearchUrl = url;
        this.indexPattern = indexPattern;
        this.intervalSeconds = intervalSeconds;
    }

    public void startOrEnsureUpdater() {
        SCHED.computeIfAbsent(opensearchUrl, u -> {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("opensearch-updater-" + Math.abs(u.hashCode()));
                return t;
            });
            Runnable task = () -> {
                try { synchronizeOnce(); }
                catch (Exception e) { log.error("OpenSearch updater error for " + opensearchUrl + ": " + e.getMessage(), e); }
            };
            scheduler.scheduleAtFixedRate(task, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
            return scheduler;
        });
    }

    private void synchronizeOnce() {
        // Refresh index list
        List<String> currentIndices = client.listIndices(indexPattern);
        if (currentIndices.isEmpty()) {
            log.info("No indices found for pattern '" + indexPattern + "' at " + opensearchUrl);
            return;
        }

        // Update OpenSearch node
        repo.ensureOpenSearchNode(opensearchUrl, indexPattern, currentIndices);

        // Update index metadata
        List<Map<String, Object>> rows = new ArrayList<>(currentIndices.size());
        for (String idx : currentIndices) {
            Map<String, String> mappings = client.getFieldMappings(idx);
            Map<String, Object> row = new HashMap<>();
            row.put("name", idx);
            row.put("docCount", 0L);
            row.put("fieldCount", mappings.size());
            rows.add(row);
        }
        repo.upsertIndicesBulk(rows, opensearchUrl);
        log.info("OpenSearch sync complete: " + currentIndices.size() + " indices for " + opensearchUrl);
    }
}
