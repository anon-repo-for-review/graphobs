package graphobs.matching;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

import java.util.*;

/**
 * Resolves graph nodes to associated entity names via graph traversal.
 * Centralizes resolution logic shared across Jaeger, OpenSearch, and other query procedures.
 */
public class NodeResolver {

    /**
     * Resolved node information for Jaeger queries.
     * Shared by all node-based Jaeger procedures (traces, spans).
     */
    public static class NodeResolution {
        /** Services to query in Jaeger. */
        public final Set<String> serviceNames;
        /** Pod names for server-side/client-side filtering. */
        public final Set<String> podNames;
        /** Operation names for server-side/client-side filtering. */
        public final Set<String> operationNames;
        /** True when Service nodes are in the input — no additional filtering needed. */
        public final boolean passAll;
        /**
         * Pod name → its own owning service.
         * Populated only when Pod nodes are in the input.
         * Used for the optimized 1-call-per-pod path (avoids N_services × N_pods cross-product).
         */
        public final Map<String, String> podToOwnService;

        public NodeResolution(Set<String> serviceNames, Set<String> podNames,
                              Set<String> operationNames, boolean passAll,
                              Map<String, String> podToOwnService) {
            this.serviceNames    = serviceNames;
            this.podNames        = podNames;
            this.operationNames  = operationNames;
            this.passAll         = passAll;
            this.podToOwnService = podToOwnService != null ? podToOwnService : Collections.emptyMap();
        }
    }

    /**
     * Resolves a list of graph nodes for Jaeger queries.
     * <ul>
     *   <li>Service → query by name, passAll=true (no post-filter needed)</li>
     *   <li>Pod → query pod's own owning service, filter by pod name in span tags</li>
     *   <li>Operation → query owning service, filter by operationName</li>
     *   <li>Server → resolve deployed pods, then query each pod's owning service</li>
     * </ul>
     *
     * For Pod nodes, both {@code podNames} and {@code podToOwnService} are populated.
     * The latter enables the optimized 1-call-per-pod path in procedures.
     */
    public static NodeResolution resolveJaegerNodes(List<Node> nodes, GraphDatabaseService db) {
        Set<String> serviceNames   = new LinkedHashSet<>();
        Set<String> podNames       = new LinkedHashSet<>();
        Set<String> operationNames = new LinkedHashSet<>();
        boolean passAll            = false;

        List<String> podElementIds         = new ArrayList<>(); // for per-pod service resolution
        List<String> unresolvedElementIds  = new ArrayList<>(); // for owning-service fallback

        for (Node node : nodes) {
            if (node == null) continue;

            if (node.hasLabel(Label.label("Service"))) {
                Object name = node.getProperty("name", null);
                if (name != null && !name.toString().isBlank()) serviceNames.add(name.toString());
                passAll = true;

            } else if (node.hasLabel(Label.label("Pod"))) {
                Object name = node.getProperty("name", null);
                if (name != null && !name.toString().isBlank()) podNames.add(name.toString());
                podElementIds.add(node.getElementId());
                unresolvedElementIds.add(node.getElementId());

            } else if (node.hasLabel(Label.label("Operation"))) {
                Object opId = node.getProperty("id", null);
                if (opId != null) {
                    String opName = OperationIdParser.extractOperationName(opId.toString());
                    if (opName != null && !opName.isBlank()) operationNames.add(opName);
                }
                unresolvedElementIds.add(node.getElementId());

            } else if (node.hasLabel(Label.label("Server"))) {
                List<String> serverPodNames = resolveServerToPodNames(node, db);
                podNames.addAll(serverPodNames);
                // For server nodes, pods are resolved by name but we can't build podToOwnService
                // without element IDs — fall through to owning-service resolution
                unresolvedElementIds.add(node.getElementId());

            } else {
                unresolvedElementIds.add(node.getElementId());
            }
        }

        // Resolve owning services for all non-Service nodes (pods, operations, servers, unknowns)
        serviceNames.addAll(resolveOwningServiceNames(unresolvedElementIds, db));

        // Build per-pod service map for the optimized 1-call-per-pod path
        Map<String, String> podToOwnService = resolvePodNamesToOwningService(podElementIds, db);

        return new NodeResolution(serviceNames, podNames, operationNames, passAll, podToOwnService);
    }

    /**
     * Resolves a Server node to the names of all Pods deployed on it.
     * Uses the relationship pattern (pod:Pod)-[:DEPLOYED_ON]->(server:Server).
     *
     * @return list of pod names, empty if none found or node is not a Server
     */
    public static List<String> resolveServerToPodNames(Node serverNode, GraphDatabaseService db) {
        if (serverNode == null) return Collections.emptyList();

        List<String> podNames = new ArrayList<>();
        try (var tx = db.beginTx()) {
            Result res = tx.execute(
                    "MATCH (pod:Pod)-[:DEPLOYED_ON]->(server) " +
                    "WHERE elementId(server) = $elementId " +
                    "RETURN pod.name AS podName",
                    Map.of("elementId", serverNode.getElementId()));
            while (res.hasNext()) {
                Object podName = res.next().get("podName");
                if (podName != null && !podName.toString().isBlank()) {
                    podNames.add(podName.toString());
                }
            }
        }
        return podNames;
    }

    /**
     * Resolves each Pod node (by element ID) to its closest owning Service name.
     * Returns a map of pod name → service name for all pods that have an owning service.
     * Used to avoid the N_services × N_pods cross-product when only Pod nodes are passed.
     *
     * @param podElementIds element IDs of Pod nodes
     * @return map of pod name → owning service name (insertion-ordered)
     */
    public static Map<String, String> resolvePodNamesToOwningService(List<String> podElementIds, GraphDatabaseService db) {
        if (podElementIds == null || podElementIds.isEmpty()) return Collections.emptyMap();

        Map<String, String> result = new LinkedHashMap<>();
        try (var tx = db.beginTx()) {
            Result res = tx.execute(
                    "UNWIND $elementIds AS eid " +
                    "MATCH (pod) WHERE elementId(pod) = eid " +
                    "MATCH path = (s:Service)-[*]->(pod) " +
                    "WITH pod, s, length(path) AS dist " +
                    "ORDER BY dist ASC " +
                    "WITH pod, collect(s)[0] AS closestService " +
                    "WHERE closestService IS NOT NULL " +
                    "RETURN pod.name AS podName, closestService.name AS serviceName",
                    Map.of("elementIds", podElementIds));
            while (res.hasNext()) {
                Map<String, Object> row = res.next();
                Object podName = row.get("podName");
                Object svcName = row.get("serviceName");
                if (podName != null && !podName.toString().isBlank() &&
                        svcName != null && !svcName.toString().isBlank()) {
                    result.put(podName.toString(), svcName.toString());
                }
            }
        }
        return result;
    }

    /**
     * Resolves a list of node element IDs to their closest owning Service names.
     * Uses the relationship pattern (s:Service)-[*]->(target) with shortest-path priority.
     *
     * @param elementIds element IDs of nodes to resolve (non-Service nodes)
     * @return set of service names (deduplicated, insertion-ordered)
     */
    public static Set<String> resolveOwningServiceNames(List<String> elementIds, GraphDatabaseService db) {
        if (elementIds == null || elementIds.isEmpty()) return Collections.emptySet();

        Set<String> serviceNames = new LinkedHashSet<>();
        try (var tx = db.beginTx()) {
            Result result = tx.execute(
                    "UNWIND $elementIds AS eid " +
                    "MATCH (target) WHERE elementId(target) = eid " +
                    "MATCH path = (s:Service)-[*]->(target) " +
                    "WITH target, s, length(path) AS dist " +
                    "ORDER BY dist ASC " +
                    "WITH target, collect(s)[0] AS closestService " +
                    "WHERE closestService IS NOT NULL " +
                    "RETURN closestService.name AS serviceName",
                    Map.of("elementIds", elementIds));
            while (result.hasNext()) {
                Object name = result.next().get("serviceName");
                if (name != null && !name.toString().isBlank()) {
                    serviceNames.add(name.toString());
                }
            }
        }
        return serviceNames;
    }
}
