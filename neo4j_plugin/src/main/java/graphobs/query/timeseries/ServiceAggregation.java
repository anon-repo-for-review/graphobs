package graphobs.query.timeseries;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import graphobs.result.TimeSeriesResult;
import graphobs.query.timeseries.TimeSeriesUtil;
import graphobs.query.timeseries.TimeWindow;
import graphobs.datasources.prometheus.PrometheusTimeSeriesSource;

import java.util.*;
import java.util.stream.Stream;

public class ServiceAggregation {

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @Procedure(name = "graphobs.data.get_all_for_service", mode = Mode.READ)
    @Description("Aggregiert eine Metrik über alle Pods eines Services. rps/cpu -> Summe, latency/error -> Durchschnitt.")
    public Stream<TimeSeriesResult> aggregatePodMetric(
            @Name("node") Node startNode,
            @Name("metric") String metric,
            @Name(value = "params", defaultValue = "{}") Map<String,Object> params
    ) {
        if (startNode == null) return Stream.empty();

        try {
            // --- 1) Pods suchen ---
            List<Node> pods = new ArrayList<>();
            for (Relationship rel : startNode.getRelationships(Direction.BOTH)) {
                Node other = rel.getOtherNode(startNode);
                if (other.hasLabel(Label.label("Pod"))) {
                    pods.add(other);
                }
            }

            if (pods.isEmpty()) {
                log.warn("aggregatePodMetric: keine Pods für Node " + startNode.getElementId() + " gefunden.");
                return Stream.empty();
            }

            // --- 2) Aggregationstyp bestimmen ---
            String promAggFunction;
            MultiSeriesAggregator.AggregationType pluginAggType;
            boolean shouldAggregate;
            switch (metric) {
                case "rps":
                case "cpu_total":
                    promAggFunction = "sum";
                    pluginAggType = MultiSeriesAggregator.AggregationType.SUM;
                    shouldAggregate = true;
                    break;
                case "latency_ms":
                case "error_rate_pct":
                    promAggFunction = "avg";
                    pluginAggType = MultiSeriesAggregator.AggregationType.AVERAGE;
                    shouldAggregate = true;
                    break;
                default:
                    promAggFunction = null;
                    pluginAggType = null;
                    shouldAggregate = false;
                    break;
            }

            // --- 3) Prometheus-Fastpath (nur fuer Metriken mit Aggregation) ---
            if (shouldAggregate) {
                Node sharedPrometheusSource = findSharedPrometheusSource(pods);
                if (sharedPrometheusSource != null) {
                    try {
                        List<String> targetNames = new ArrayList<>();
                        for (Node pod : pods) {
                            String name = (String) pod.getProperty("name", "");
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
                                    log.info("aggregatePodMetric: Prometheus-Fastpath erfolgreich (%s ueber %d Pods)", promAggFunction, targetNames.size());
                                    return Stream.of(result);
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.info("aggregatePodMetric: Prometheus-Fastpath fehlgeschlagen, Fallback: %s", e.getMessage());
                    }
                }
            }

            // --- 4) Fallback: Batch-Fetch + Plugin-seitige Aggregation (bisheriges Verhalten) ---
            Map<Node, List<TimeSeriesResult>> batchResults =
                    TimeSeriesUtil.getBatchFilteredTimeSeries(pods, metric, params, db, log);
            List<TimeSeriesResult> seriesList = new ArrayList<>();
            for (Node pod : pods) {
                List<TimeSeriesResult> podResults = batchResults.get(pod);
                if (podResults != null) seriesList.addAll(podResults);
            }

            if (seriesList.isEmpty()) {
                return Stream.empty();
            }

            // --- 5) Aggregation an den MultiSeriesAggregator delegieren ---
            if (shouldAggregate) {
                return Stream.of(
                        MultiSeriesAggregator.aggregate(seriesList, metric, pluginAggType)
                );
            } else {
                // Keine Aggregation -> Alle einzeln zurückgeben (wie bisher)
                return seriesList.stream();
            }

        } catch (Exception e) {
            log.error("Fehler in aggregatePodMetric: " + e.getMessage());
            return Stream.empty();
        }
    }

    /**
     * Prueft ob ALLE Knoten aus derselben Prometheus-Instanz kommen.
     * @return Der gemeinsame Prometheus-Node, oder null wenn nicht alle denselben teilen.
     */
    private Node findSharedPrometheusSource(List<Node> pods) {
        Node sharedSource = null;
        for (Node pod : pods) {
            if (pod == null) continue;
            if (pod.hasLabel(Label.label("Service")) || pod.hasLabel(Label.label("Operation"))) return null;

            Node promSource = null;
            try {
                for (Relationship rel : pod.getRelationships(Direction.OUTGOING, RelationshipType.withName("HAS_TIME_SERIES"))) {
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
                return null;
            }
        }
        return sharedSource;
    }
}
