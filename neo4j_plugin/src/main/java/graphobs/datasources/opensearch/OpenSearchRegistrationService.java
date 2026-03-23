package graphobs.datasources.opensearch;

import graphobs.matching.FieldNameResolver;
import org.neo4j.logging.Log;

import java.util.*;

public class OpenSearchRegistrationService {
    private final OpenSearchClient client;
    private final LogRepository repo;
    private final Log log;

    public OpenSearchRegistrationService(OpenSearchClient client, LogRepository repo, Log log) {
        this.client = client;
        this.repo = repo;
        this.log = log;
    }

    public long register(String url, String indexPattern) throws Exception {
        // 1) Ping OpenSearch
        if (!client.ping()) {
            throw new RuntimeException("OpenSearch not reachable at " + url);
        }
        log.info("OpenSearch reachable at " + url);

        // 2) List indices matching pattern
        List<String> indices = client.listIndices(indexPattern);
        log.info("Found " + indices.size() + " indices matching '" + indexPattern + "'");

        // 3) Ensure OpenSearch node in Neo4j
        repo.ensureOpenSearchNode(url, indexPattern, indices);

        // Severity and message field candidates
        String[] severityCandidates = {"severity.text", "severity_text", "level", "log.level"};
        String[] messageCandidates = {"body", "message", "msg", "log"};

        // 4) For each index: get field mappings, resolve field names, store metadata
        List<Map<String, Object>> rows = new ArrayList<>(indices.size());
        for (String idx : indices) {
            Map<String, String> mappings = client.getFieldMappings(idx);
            Set<String> fieldNames = mappings.keySet();

            Map<String, Object> row = new HashMap<>();
            row.put("name", idx);
            row.put("docCount", 0L); // will be updated by periodic sync
            row.put("fieldCount", mappings.size());

            // Resolve pod field: find first candidate that exists in mapping
            row.put("podField", resolveFirstMatch(fieldNames, FieldNameResolver.OPENSEARCH_POD_FIELDS));
            // Resolve service field
            row.put("serviceField", resolveFirstMatch(fieldNames, FieldNameResolver.OPENSEARCH_SERVICE_FIELDS));
            // Resolve severity field
            row.put("severityField", resolveFirstMatch(fieldNames, severityCandidates));
            // Resolve message field
            row.put("messageField", resolveFirstMatch(fieldNames, messageCandidates));

            rows.add(row);
        }
        repo.upsertIndicesBulk(rows, url);

        return indices.size();
    }

    /**
     * Returns the first candidate that exists in the field names set (checking both exact and dot-prefix variants).
     */
    private static String resolveFirstMatch(Set<String> fieldNames, String[] candidates) {
        for (String candidate : candidates) {
            if (fieldNames.contains(candidate)) return candidate;
            // Also check with .keyword suffix stripped (mappings may list "resource.k8s.pod.name" not "resource.k8s.pod.name.keyword")
            // And check with dot-separated prefix (e.g. "severity.text" may appear as "severity.text" in mappings)
        }
        // Second pass: check if any field name starts with a candidate (for nested fields that appear flattened)
        for (String candidate : candidates) {
            for (String field : fieldNames) {
                if (field.equals(candidate) || field.equals(candidate + ".keyword")) {
                    return candidate;
                }
            }
        }
        return null;
    }
}
