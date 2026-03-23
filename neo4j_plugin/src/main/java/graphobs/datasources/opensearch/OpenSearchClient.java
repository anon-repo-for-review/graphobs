package graphobs.datasources.opensearch;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

public interface OpenSearchClient {
    /** Checks if OpenSearch is reachable. */
    boolean ping();

    /** Lists all indices matching the pattern (e.g. "otel-logs-*"). */
    List<String> listIndices(String pattern);

    /** Returns cluster health info. */
    String getClusterHealth();

    /** Executes a search and returns raw JSON. */
    JsonNode search(String index, JsonNode queryBody);

    /** Returns the field mappings of an index. */
    Map<String, String> getFieldMappings(String index);
}
