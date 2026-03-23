package graphobs.datasources.opensearch;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class OpenSearchRegistrar {
    @Context public GraphDatabaseService db;
    @Context public Log log;

    public static class Output {
        public String message;
        public String url;
        public long indexCount;
        public Output(String m, String u, long c) { message = m; url = u; indexCount = c; }
    }

    @Procedure(name = "graphobs.datasources.register_opensearch", mode = Mode.WRITE)
    public Stream<Output> registerOpenSearch(
            @Name("url") String url,
            @Name(value = "options", defaultValue = "{}") Map<String, Object> options) {
        try {
            String indexPattern = (options != null && options.get("index") instanceof String)
                    ? (String) options.get("index") : "*";
            String user = (options != null && options.get("user") instanceof String)
                    ? (String) options.get("user") : null;
            String password = (options != null && options.get("password") instanceof String)
                    ? (String) options.get("password") : null;

            OpenSearchClient client = new OpenSearchHttpClient(url, user, password, log);
            LogRepository repo = new Neo4jLogRepository(db);
            OpenSearchRegistrationService svc = new OpenSearchRegistrationService(client, repo, log);
            long count = svc.register(url, indexPattern);

            // Store credentials on OpenSearch node so get_logs can read them
            if (user != null) {
                try (var tx = db.beginTx()) {
                    Map<String, Object> credParams = new HashMap<>();
                    credParams.put("url", url);
                    credParams.put("user", user);
                    credParams.put("password", password);
                    tx.execute("MATCH (os:OpenSearch {url:$url}) SET os.user=$user, os.password=$password", credParams);
                    tx.commit();
                }
            }

            boolean doUpdate = (options != null && options.getOrDefault("update", null) instanceof Boolean)
                    ? (Boolean) options.get("update") : false;

            if (doUpdate) {
                long interval = (options != null && options.get("intervalSeconds") instanceof Number)
                        ? ((Number) options.get("intervalSeconds")).longValue() : 60L;
                OpenSearchToNeo4jUpdater up = new OpenSearchToNeo4jUpdater(client, repo, log, url, indexPattern, interval);
                up.startOrEnsureUpdater();
            }

            return Stream.of(new Output("registered", url, count));
        } catch (Exception e) {
            return Stream.of(new Output("error: " + e.getMessage(), url, 0));
        }
    }
}
