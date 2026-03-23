package graphobs.query.timeseries.aggregations;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import graphobs.result.StatisticResult;
import graphobs.result.TimeSeriesResult;
import graphobs.query.timeseries.TimeSeriesUtil;

import java.util.*;
import java.util.stream.Stream;

public class Sum_Count {

    /*@Procedure(name = "decrapted.aggregation.sum", mode = Mode.READ)
    @Description("Gibt die Summe aller Werte für jede Property zurück")
    public Stream<StatisticResult> sumTimeSeries(@Name("tsNode") Node tsNode) {
        String[] timestamps = (String[]) tsNode.getProperty("timestamps", new String[0]);
        Map<String, Object[]> raw = TimeSeriesUtil.getValueProperties(tsNode, timestamps.length);

        Map<String, Double> summed = new HashMap<>();
        for (Map.Entry<String, Object[]> entry : raw.entrySet()) {
            double sum = Arrays.stream(entry.getValue())
                    .mapToDouble(o -> ((Number) o).doubleValue())
                    .sum();
            summed.put(entry.getKey(), sum); // nur ein Wert: die Gesamtsumme
        }

        return Stream.of(new StatisticResult(summed));
    }


    @Procedure(name = "decrapted.aggregation.count", mode = Mode.READ)
    @Description("Zählt die Anzahl der vorhandenen Werte pro Property")
    public Stream<StatisticResult> countTimeSeries(@Name("tsNode") Node tsNode) {
        String[] timestamps = (String[]) tsNode.getProperty("timestamps", new String[0]);
        Map<String, Object[]> raw = TimeSeriesUtil.getValueProperties(tsNode, timestamps.length);

        Map<String, Double> counts = new HashMap<>();
        for (Map.Entry<String, Object[]> entry : raw.entrySet()) {
            long count = Arrays.stream(entry.getValue())
                    .filter(Objects::nonNull)
                    .count();
            counts.put(entry.getKey(), ((double) count)); // Nur ein Zählwert
        }

        return Stream.of(new StatisticResult(counts));
    }*/

}
