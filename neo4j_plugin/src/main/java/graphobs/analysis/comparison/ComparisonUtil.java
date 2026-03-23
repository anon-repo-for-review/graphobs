package graphobs.analysis.comparison;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import graphobs.result.TimeSeriesResult;
import graphobs.query.timeseries.TimeSeriesUtil;

import java.util.*;
import java.util.stream.Collectors;

public class ComparisonUtil {

    public static List<Double> computeMeansForNodeAndPeriod(Node node, String metric, Map<String,Object> periodOptions, GraphDatabaseService db, Log log) {
        List<Double> means = new ArrayList<>();
        try {
            // hole alle relevanten TimeSeriesResults
            List<TimeSeriesResult> results = TimeSeriesUtil
                    .getFilteredTimeSeries(node, metric, periodOptions, db, log)
                    .collect(Collectors.toList());

            for (TimeSeriesResult tsResult : results) {
                if (tsResult == null || tsResult.values == null || tsResult.values.isEmpty()) continue;

                List<Double> values = tsResult.values.get(metric);
                if (values == null || values.isEmpty()) {
                    // fallback: erste Serie nehmen
                    if (!tsResult.values.isEmpty()) {
                        values = tsResult.values.values().iterator().next();
                    }
                }

                if (values != null && !values.isEmpty()) {
                    List<Double> cleaned = values.stream()
                            .filter(Objects::nonNull)
                            .map(v -> (v.isNaN() || v.isInfinite()) ? 0.0 : v)
                            .collect(Collectors.toList());

                    if (!cleaned.isEmpty()) {
                        double mean = meanFromList(cleaned);
                        if (!Double.isNaN(mean)) {
                            means.add(mean);
                        } else {
                            log.info("Computed NaN mean for node " + node.getElementId()
                                    + " metric " + metric
                                    + " for period options: " + periodOptions);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Error computing means for node " + node.getElementId()
                    + " metric " + metric + ": " + e.getMessage());
        }
        return means;
    }


    /**
     * Batch version: computes means for all nodes in as few Prometheus queries as possible.
     * @return Map from Node to list of mean values (same semantics as computeMeansForNodeAndPeriod per node)
     */
    public static Map<Node, List<Double>> computeMeansBatch(List<Node> nodes, String metric, Map<String, Object> periodOptions, GraphDatabaseService db, Log log) {
        Map<Node, List<Double>> result = new LinkedHashMap<>();

        try {
            Map<Node, List<TimeSeriesResult>> batchResults = TimeSeriesUtil.getBatchFilteredTimeSeries(nodes, metric, periodOptions, db, log);

            for (Node node : nodes) {
                List<Double> means = new ArrayList<>();
                List<TimeSeriesResult> tsResults = batchResults.get(node);
                if (tsResults != null) {
                    for (TimeSeriesResult tsResult : tsResults) {
                        if (tsResult == null || tsResult.values == null || tsResult.values.isEmpty()) continue;

                        List<Double> values = tsResult.values.get(metric);
                        if (values == null || values.isEmpty()) {
                            if (!tsResult.values.isEmpty()) {
                                values = tsResult.values.values().iterator().next();
                            }
                        }

                        if (values != null && !values.isEmpty()) {
                            List<Double> cleaned = values.stream()
                                    .filter(Objects::nonNull)
                                    .map(v -> (v.isNaN() || v.isInfinite()) ? 0.0 : v)
                                    .collect(Collectors.toList());

                            if (!cleaned.isEmpty()) {
                                double mean = meanFromList(cleaned);
                                if (!Double.isNaN(mean)) {
                                    means.add(mean);
                                }
                            }
                        }
                    }
                }
                result.put(node, means);
            }
        } catch (Exception e) {
            log.warn("Batch means computation failed, falling back to single: " + e.getMessage());
            // Fallback: compute individually
            for (Node node : nodes) {
                result.put(node, computeMeansForNodeAndPeriod(node, metric, periodOptions, db, log));
            }
        }

        return result;
    }

    private static double meanFromList(List<Double> list) {
        if (list == null || list.isEmpty()) return Double.NaN;
        double s = 0.0;
        for (Double d : list) {
            if (d == null || d.isNaN() || d.isInfinite()) continue;
            s += d;
        }
        return s / list.size();
    }
}
