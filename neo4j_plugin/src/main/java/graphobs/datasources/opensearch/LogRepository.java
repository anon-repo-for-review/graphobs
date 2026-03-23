package graphobs.datasources.opensearch;

import java.util.List;
import java.util.Map;

public interface LogRepository {
    long ensureOpenSearchNode(String url, String indexPattern, List<String> indices);
    void upsertIndicesBulk(List<Map<String, Object>> rows, String opensearchUrl);
    Map<String, Object> findOpenSearchConfig(String url);
}
