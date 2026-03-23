package graphobs.query.timeseries.aggregations;


import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.*;
import graphobs.result.StatisticResult;
import graphobs.query.timeseries.TimeSeriesUtil;

import java.util.*;
import java.util.stream.Stream;

//import static graphobs.query.timeseries.TimeSeriesUtil.validateTimeSeries;

public class Statistic {

    /*@Procedure(name = "decrapted.aggregation.means", mode = Mode.READ)
    @Description("Berechnet die Mittelwerte aller numerischen Properties")
    public Stream<StatisticResult> calculateMeans(@Name("tsNode") Node tsNode) {
        validateTimeSeries(tsNode);
        String[] timestamps = (String[]) tsNode.getProperty("timestamps", new String[0]);
        int n = timestamps.length;

        Map<String, Object[]> values = TimeSeriesUtil.getValueProperties(tsNode, n);
        Map<String, Double> resultMap = new HashMap<>();

        for (Map.Entry<String, Object[]> entry : values.entrySet()) {
            Object[] array = entry.getValue();

            double sum = 0;
            int count = 0;

            for (Object val : array) {
                if (val instanceof Number num) {
                    sum += num.doubleValue();
                    count++;
                }
            }

            if (count > 0) {
                resultMap.put(entry.getKey(), sum / count);
            }
        }

        return Stream.of(new StatisticResult(resultMap));
    }



    @Procedure(name = "decrapted.aggregation.std", mode = Mode.READ)
    @Description("Berechnet die Standardabweichung aller numerischen Properties")
    public Stream<StatisticResult> calculateStdDev(@Name("tsNode") Node tsNode) {
        validateTimeSeries(tsNode);
        String[] timestamps = (String[]) tsNode.getProperty("timestamps", new String[0]);
        int n = timestamps.length;

        Map<String, Object[]> values = TimeSeriesUtil.getValueProperties(tsNode, n);
        Map<String, Double> resultMap = new HashMap<>();

        for (Map.Entry<String, Object[]> entry : values.entrySet()) {
            Object[] array = entry.getValue();

            List<Double> numbers = Arrays.stream(array)
                    .filter(o -> o instanceof Number)
                    .map(o -> ((Number) o).doubleValue())
                    .toList();

            if (numbers.isEmpty()) continue;

            double mean = numbers.stream().mapToDouble(d -> d).average().orElse(0);
            double variance = numbers.stream()
                    .mapToDouble(d -> (d - mean) * (d - mean))
                    .average().orElse(0);
            double stddev = Math.sqrt(variance);

            resultMap.put(entry.getKey(), stddev);
        }

        return Stream.of(new StatisticResult(resultMap));
    }


    /*@Procedure(name = "decrapted.aggregation.median", mode = Mode.READ)
    @Description("Berechnet den Median aller numerischen Properties")
    public Stream<StatisticResult> calculateMedian(@Name("tsNode") Node tsNode) {
        validateTimeSeries(tsNode);
        String[] timestamps = (String[]) tsNode.getProperty("timestamps", new String[0]);
        int n = timestamps.length;

        Map<String, Object[]> values = TimeSeriesUtil.getValueProperties(tsNode, n);
        Map<String, Double> resultMap = new HashMap<>();

        for (Map.Entry<String, Object[]> entry : values.entrySet()) {
            Object[] array = entry.getValue();

            double[] numbers = Arrays.stream(array)
                    .filter(o -> o instanceof Number)
                    .mapToDouble(o -> ((Number) o).doubleValue())
                    .sorted()
                    .toArray();

            if (numbers.length == 0) continue;

            double median = (numbers.length % 2 == 0)
                    ? (numbers[numbers.length / 2 - 1] + numbers[numbers.length / 2]) / 2.0
                    : numbers[numbers.length / 2];

            resultMap.put(entry.getKey(), median);
        }

        return Stream.of(new StatisticResult(resultMap));
    }


    @Procedure(name = "decrapted.aggregation.percentile", mode = Mode.READ)
    @Description("Gibt das gewünschte Perzentil pro Property zurück (z.B. 0.5 = Median)")
    public Stream<StatisticResult> percentileTimeSeries(
            @Name("tsNode") Node tsNode,
            @Name("percentile") double p
    ) {
        if (p < 0.0 || p > 1.0) {
            throw new IllegalArgumentException("Perzentil muss zwischen 0.0 und 1.0 liegen");
        }

        String[] timestamps = (String[]) tsNode.getProperty("timestamps", new String[0]);
        Map<String, Object[]> raw = TimeSeriesUtil.getValueProperties(tsNode, timestamps.length);

        Map<String, Double> result = new HashMap<>();
        for (Map.Entry<String, Object[]> entry : raw.entrySet()) {
            double[] sorted = Arrays.stream(entry.getValue())
                    .filter(Objects::nonNull)
                    .mapToDouble(o -> ((Number) o).doubleValue())
                    .sorted()
                    .toArray();

            int n = sorted.length;
            if (n == 0) continue;

            double index = p * (n - 1);
            int lower = (int) Math.floor(index);
            int upper = (int) Math.ceil(index);
            double percentileValue = sorted[lower] + (sorted[upper] - sorted[lower]) * (index - lower);

            result.put(entry.getKey(), percentileValue);
        }

        return Stream.of(new StatisticResult(result));
    }*/
}