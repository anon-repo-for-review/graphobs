package graphobs.query.timeseries;


import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import graphobs.result.TimeSeriesResult;
import graphobs.query.timeseries.TimeSeriesUtil;
import graphobs.query.timeseries.TimeWindow;
import graphobs.datasources.prometheus.PrometheusTimeSeriesSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class TopologicalAggregation {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(name = "graphobs.aggregation.aggregate_nodes", mode = Mode.READ)
    @Description("Aggregiert eine Metrik über eine beliebige Liste von Knoten (Topologie). " +
            "Gibt ein TimeSeriesResult zurück. Parameter 'aggregation' kann 'sum' (default) oder 'avg' sein.")
    public Stream<TimeSeriesResult> aggregateNodes(
            @Name("nodes") List<Node> nodes,
            @Name("metric") String metric,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        if (nodes == null || nodes.isEmpty()) {
            return Stream.empty();
        }
        if (metric == null || metric.isBlank()) {
            log.warn("aggregate_nodes: Keine Metrik angegeben.");
            return Stream.empty();
        }

        try {
            // 1. Aggregations-Typ bestimmen (Default: SUM)
            String aggParam = (String) params.getOrDefault("top_aggregation", "sum");
            MultiSeriesAggregator.AggregationType type;
            String promAggFunction;
            if (aggParam.equalsIgnoreCase("avg") || aggParam.equalsIgnoreCase("average") || aggParam.equalsIgnoreCase("mean")) {
                type = MultiSeriesAggregator.AggregationType.AVERAGE;
                promAggFunction = "avg";
            } else {
                type = MultiSeriesAggregator.AggregationType.SUM;
                promAggFunction = "sum";
            }

            // 2. Prometheus-Fastpath: Wenn alle Knoten aus derselben Prometheus-Instanz kommen,
            //    kann Prometheus die Aggregation serverseitig durchfuehren (1 Query statt N Serien + Plugin-Aggregation)
            Node sharedPrometheusSource = findSharedPrometheusSource(nodes);
            if (sharedPrometheusSource != null) {
                try {
                    List<String> targetNames = new ArrayList<>();
                    for (Node node : nodes) {
                        if (node == null) continue;
                        String name = (String) node.getProperty("name", "");
                        if (!name.isEmpty()) targetNames.add(name);
                    }

                    if (!targetNames.isEmpty()) {
                        TimeWindow window = TimeSeriesUtil.extractTimeWindow(params, log);
                        PrometheusTimeSeriesSource source = new PrometheusTimeSeriesSource();
                        Optional<TimeSeriesResult> fastResult = source.fetchAggregated(
                                sharedPrometheusSource, targetNames, metric, window, log, params, promAggFunction);

                        if (fastResult.isPresent()) {
                            TimeSeriesResult result = fastResult.get();
                            if (result.timestamps != null && !result.timestamps.isEmpty()) {
                                log.info("aggregate_nodes: Prometheus-Fastpath erfolgreich (%s ueber %d Knoten)", promAggFunction, targetNames.size());
                                return Stream.of(result);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.info("aggregate_nodes: Prometheus-Fastpath fehlgeschlagen, Fallback auf Plugin-Aggregation: %s", e.getMessage());
                }
            }

            // 3. Fallback: Batch-Fetch + Plugin-seitige Aggregation (bisheriges Verhalten)
            Map<Node, List<TimeSeriesResult>> batchResults =
                    TimeSeriesUtil.getBatchFilteredTimeSeries(nodes, metric, params, db, log);
            List<TimeSeriesResult> allSeries = new ArrayList<>();
            for (Node node : nodes) {
                if (node == null) continue;
                List<TimeSeriesResult> nodeResults = batchResults.get(node);
                if (nodeResults != null) allSeries.addAll(nodeResults);
            }

            if (allSeries.isEmpty()) {
                log.info("aggregate_nodes: Keine Zeitreihen für die angegebenen Knoten gefunden.");
                return Stream.empty();
            }

            for (int i=0; i<allSeries.size(); i++) {
                log.info("ts: " + i + ": " + allSeries.get(i).values.toString());
            }

            // 4. Aggregation durchführen
            TimeSeriesResult result = MultiSeriesAggregator.aggregate(allSeries, metric, type);

            log.info("result ts: " + result.values.toString());

            if (result.timestamps == null || result.timestamps.isEmpty()) {
                return Stream.empty();
            }

            return Stream.of(result);

        } catch (Exception e) {
            log.error("Fehler in timegraph.aggregation.aggregate_nodes: " + e.getMessage(), e);
            return Stream.empty();
        }
    }

    /**
     * Prueft ob ALLE Knoten aus derselben Prometheus-Instanz kommen.
     * @return Der gemeinsame Prometheus-Node, oder null wenn nicht alle denselben teilen.
     */
    private Node findSharedPrometheusSource(List<Node> nodes) {
        Node sharedSource = null;
        for (Node node : nodes) {
            if (node == null) continue;
            // Skip Service/Operation nodes (die haben keinen direkten Prometheus-Pfad fuer Fastpath)
            if (node.hasLabel(Label.label("Service")) || node.hasLabel(Label.label("Operation"))) return null;

            Node promSource = null;
            try {
                for (Relationship rel : node.getRelationships(Direction.OUTGOING, RelationshipType.withName("HAS_TIME_SERIES"))) {
                    Node target = rel.getEndNode();
                    if (target.hasLabel(Label.label("Prometheus"))) {
                        promSource = target;
                        break;
                    }
                }
            } catch (Exception e) {
                return null;
            }

            if (promSource == null) return null;
            if (sharedSource == null) {
                sharedSource = promSource;
            } else if (!sharedSource.getElementId().equals(promSource.getElementId())) {
                return null; // verschiedene Prometheus-Instanzen
            }
        }
        return sharedSource;
    }
}
