package graphobs.datasources.opensearch;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.*;

public class Neo4jLogRepository implements LogRepository {
    private final GraphDatabaseService db;

    public Neo4jLogRepository(GraphDatabaseService db) {
        this.db = db;
    }

    @Override
    public long ensureOpenSearchNode(String url, String indexPattern, List<String> indices) {
        Map<String, Object> params = new HashMap<>();
        params.put("url", url);
        params.put("indexPattern", indexPattern);
        params.put("indexCount", indices.size());
        try (Transaction tx = db.beginTx()) {
            Result r = tx.execute(
                    "MERGE (os:OpenSearch {url:$url}) " +
                    "SET os.indexPattern=$indexPattern, os.indexCount=$indexCount " +
                    "RETURN id(os) AS id", params);
            long id = r.hasNext() ? (long) r.next().get("id") : -1;
            tx.commit();
            return id;
        }
    }

    @Override
    public void upsertIndicesBulk(List<Map<String, Object>> rows, String opensearchUrl) {
        if (rows == null || rows.isEmpty()) return;
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> params = new HashMap<>();
            params.put("rows", rows);
            params.put("url", opensearchUrl);
            String q = ""
                    + "UNWIND $rows AS r\n"
                    + "MERGE (idx:LogIndex {name:r.name})\n"
                    + "SET idx.docCount = r.docCount,\n"
                    + "    idx.fieldCount = r.fieldCount\n"
                    + "SET idx.podField = CASE WHEN r.podField IS NOT NULL THEN r.podField ELSE idx.podField END\n"
                    + "SET idx.serviceField = CASE WHEN r.serviceField IS NOT NULL THEN r.serviceField ELSE idx.serviceField END\n"
                    + "SET idx.severityField = CASE WHEN r.severityField IS NOT NULL THEN r.severityField ELSE idx.severityField END\n"
                    + "SET idx.messageField = CASE WHEN r.messageField IS NOT NULL THEN r.messageField ELSE idx.messageField END\n"
                    + "WITH idx\n"
                    + "MATCH (os:OpenSearch {url:$url})\n"
                    + "MERGE (idx)-[:STORED_IN]->(os)";
            tx.execute(q, params);
            tx.commit();
        }
    }

    @Override
    public Map<String, Object> findOpenSearchConfig(String url) {
        try (Transaction tx = db.beginTx()) {
            Result r = tx.execute(
                    "MATCH (os:OpenSearch {url:$url}) RETURN os.indexPattern AS indexPattern, os.indexCount AS indexCount",
                    Collections.singletonMap("url", url));
            if (r.hasNext()) {
                Map<String, Object> row = r.next();
                tx.commit();
                return row;
            }
            tx.commit();
        }
        return Collections.emptyMap();
    }
}
