package graphobs.matching;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.Map;

public class ServicePodMatcher {

    /**
     * Returns true if the pod name indicates it belongs to the given service.
     * Primary strategy: pod name starts with service name + "-" (Kubernetes convention).
     * Fallback: pod name contains service name + "-".
     */
    /*public static boolean podBelongsToService(String podName, String serviceName) {
        if (podName == null || serviceName == null) return false;
        String normPod = EntityNameNormalizer.normalizePodName(podName);
        String normSvc = EntityNameNormalizer.normalizeServiceName(serviceName);
        if (normPod == null || normSvc == null) return false;
        return normPod.startsWith(normSvc + "-") || normPod.contains(normSvc + "-");
    }*/

    /**
     * Bulk-links Service nodes to Pod nodes in Neo4j using STARTS WITH matching
     * with longest-match priority. Falls back to CONTAINS for unmatched pods.
     * Returns the total number of relationships created.
     */
    public static long linkServicesAndPods(GraphDatabaseService db, Log log) {
        long totalCount = 0;

        // Primary: STARTS WITH (precise Kubernetes naming)
        String startsWithQuery =
                "MATCH (s:Service), (p:Pod) " +
                "WHERE p.name STARTS WITH (s.name + '-') " +
                "WITH p, s " +
                "ORDER BY size(s.name) DESC " +
                "WITH p, collect(s) as potentialServices " +
                "WITH p, head(potentialServices) as bestService " +
                "MERGE (bestService)-[:HAS_POD]->(p) " +
                "RETURN count(*) as createdRels";

        try (Transaction tx = db.beginTx()) {
            org.neo4j.graphdb.Result res = tx.execute(startsWithQuery);
            if (res.hasNext()) {
                Object val = res.next().get("createdRels");
                if (val instanceof Number) {
                    totalCount = ((Number) val).longValue();
                }
            }
            tx.commit();
        } catch (Exception e) {
            if (log != null) log.error("ServicePodMatcher: Error in STARTS WITH linking", e);
        }

        // Fallback: CONTAINS for pods not yet linked (e.g. "socialnetwork_media-service_1")
        String containsQuery =
                "MATCH (s:Service), (p:Pod) " +
                "WHERE NOT (s)-[:HAS_POD]->(p) " +
                "AND NOT ()-[:HAS_POD]->(p) " +
                "AND p.name CONTAINS (s.name + '-') " +
                "WITH p, s " +
                "ORDER BY size(s.name) DESC " +
                "WITH p, collect(s) as potentialServices " +
                "WITH p, head(potentialServices) as bestService " +
                "MERGE (bestService)-[:HAS_POD]->(p) " +
                "RETURN count(*) as createdRels";

        try (Transaction tx = db.beginTx()) {
            org.neo4j.graphdb.Result res = tx.execute(containsQuery);
            if (res.hasNext()) {
                Object val = res.next().get("createdRels");
                if (val instanceof Number) {
                    totalCount += ((Number) val).longValue();
                }
            }
            tx.commit();
        } catch (Exception e) {
            if (log != null) log.error("ServicePodMatcher: Error in CONTAINS fallback linking", e);
        }

        if (log != null) {
            if (totalCount > 0) {
                log.info("ServicePodMatcher: Linked " + totalCount + " Services to Pods.");
            } else {
                log.info("ServicePodMatcher: No new links created.");
            }
        }

        return totalCount;
    }

    /**
     * Links all Service and Operation nodes to the Prometheus node.
     */
    public static void linkServicesAndOperationsToPrometheus(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            String q = "MATCH (x), (p:Prometheus) " +
                    "WHERE (x:Service OR x:Operation) " +
                    "MERGE (x)-[:HAS_TIME_SERIES]->(p)";
            tx.execute(q);
            tx.commit();
        }
    }
}
