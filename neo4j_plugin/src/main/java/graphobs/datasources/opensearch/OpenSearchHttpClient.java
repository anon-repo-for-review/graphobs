package graphobs.datasources.opensearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.logging.Log;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

public final class OpenSearchHttpClient implements OpenSearchClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String baseUrl;
    private final String authHeader; // nullable
    private final Log log;
    private final HttpClient httpClient;

    public OpenSearchHttpClient(String baseUrl, String user, String password, Log log) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        this.log = log;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        if (user != null && !user.isBlank()) {
            String credentials = user + ":" + (password != null ? password : "");
            this.authHeader = "Basic " + Base64.getEncoder().encodeToString(
                    credentials.getBytes(StandardCharsets.UTF_8));
        } else {
            this.authHeader = null;
        }
    }

    private HttpRequest.Builder requestBuilder(String url) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json");
        if (authHeader != null) builder.header("Authorization", authHeader);
        return builder;
    }

    private JsonNode httpGet(String url) throws Exception {
        HttpRequest req = requestBuilder(url).GET().build();
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        return MAPPER.readTree(resp.body());
    }

    private JsonNode httpPost(String url, JsonNode body) throws Exception {
        HttpRequest req = requestBuilder(url)
                .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
                .build();
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 300) {
            log.warn("OpenSearch POST " + url + " returned " + resp.statusCode() + ": " + resp.body());
        }
        return MAPPER.readTree(resp.body());
    }

    @Override
    public boolean ping() {
        try {
            JsonNode root = httpGet(baseUrl);
            return root != null && root.has("cluster_name");
        } catch (Exception e) {
            log.warn("OpenSearch ping failed: " + e.getMessage());
            return false;
        }
    }

    @Override
    public List<String> listIndices(String pattern) {
        try {
            String url = baseUrl + "_cat/indices/" + pattern + "?format=json&h=index,docs.count";
            JsonNode root = httpGet(url);
            List<String> indices = new ArrayList<>();
            if (root != null && root.isArray()) {
                for (JsonNode node : root) {
                    String idx = node.path("index").asText(null);
                    if (idx != null && !idx.startsWith(".")) {
                        indices.add(idx);
                    }
                }
            }
            return indices;
        } catch (Exception e) {
            log.warn("OpenSearch listIndices failed: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    public String getClusterHealth() {
        try {
            JsonNode root = httpGet(baseUrl + "_cluster/health");
            return root != null ? root.path("status").asText("unknown") : "unknown";
        } catch (Exception e) {
            return "unreachable";
        }
    }

    @Override
    public JsonNode search(String index, JsonNode queryBody) {
        try {
            String url = baseUrl + index + "/_search";
            return httpPost(url, queryBody);
        } catch (Exception e) {
            log.error("OpenSearch search failed for " + index + ": " + e.getMessage());
            return null;
        }
    }

    @Override
    public Map<String, String> getFieldMappings(String index) {
        try {
            String url = baseUrl + index + "/_mapping";
            JsonNode root = httpGet(url);
            Map<String, String> mappings = new LinkedHashMap<>();
            if (root != null) {
                // Response: { "index-name": { "mappings": { "properties": { "field": { "type": "..." } } } } }
                Iterator<JsonNode> indexNodes = root.elements();
                if (indexNodes.hasNext()) {
                    JsonNode props = indexNodes.next().path("mappings").path("properties");
                    flattenMappings("", props, mappings);
                }
            }
            return mappings;
        } catch (Exception e) {
            log.warn("OpenSearch getFieldMappings failed for " + index + ": " + e.getMessage());
            return Collections.emptyMap();
        }
    }

    private void flattenMappings(String prefix, JsonNode node, Map<String, String> out) {
        if (node == null || !node.isObject()) return;
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String fullName = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            JsonNode val = entry.getValue();
            if (val.has("type")) {
                out.put(fullName, val.get("type").asText());
            }
            if (val.has("properties")) {
                flattenMappings(fullName, val.get("properties"), out);
            }
        }
    }
}
