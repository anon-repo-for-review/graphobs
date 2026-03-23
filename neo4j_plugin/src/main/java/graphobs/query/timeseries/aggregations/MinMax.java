package graphobs.query.timeseries.aggregations;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.*;
import graphobs.result.StatisticResult;
import graphobs.query.timeseries.TimeSeriesUtil;

import java.util.*;
import java.util.stream.Stream;

//import static graphobs.query.timeseries.TimeSeriesUtil.getValueProperties;
//import static graphobs.query.timeseries.TimeSeriesUtil.validateTimeSeries;

public class MinMax {

    /*@Procedure(name = "decrapted.aggregation.calculate_max", mode = Mode.READ)
    @Description("Berechnet den Maximalwert pro Zeitreihe (Property) in einem TimeSeries-Knoten")
    public Stream<StatisticResult> calculateMax(@Name("tsNode") Node tsNode) {
        validateTimeSeries(tsNode);
        String[] timestamps = (String[]) tsNode.getProperty("timestamps", new String[0]);
        int n = timestamps.length;

        // Holt alle gültigen Zeitreihen-Properties
        Map<String, Object[]> values = TimeSeriesUtil.getValueProperties(tsNode, n);

        Map<String, Double> resultMap = new HashMap<>();

        for (Map.Entry<String, Object[]> entry : values.entrySet()) {
            String key = entry.getKey();
            Object[] array = entry.getValue();

            double max = Double.NEGATIVE_INFINITY;
            for (Object val : array) {
                if (val instanceof Number) {
                    double d = ((Number) val).doubleValue();
                    if (d > max) max = d;
                }
            }

            resultMap.put(key, max);
        }

        StatisticResult result = new StatisticResult(resultMap);
        return Stream.of(result);
    }


    @Procedure(name = "decrapted.aggregation.calculate_min", mode = Mode.READ)
    @Description("Berechnet den Minimalwert pro Zeitreihe (Property) in einem TimeSeries-Knoten")
    public Stream<StatisticResult> calculateMin(@Name("tsNode") Node tsNode) {
        validateTimeSeries(tsNode);
        String[] timestamps = (String[]) tsNode.getProperty("timestamps", new String[0]);
        int n = timestamps.length;

        // Holt alle gültigen Zeitreihen-Properties
        Map<String, Object[]> values = TimeSeriesUtil.getValueProperties(tsNode, n);

        Map<String, Double> resultMap = new HashMap<>();

        for (Map.Entry<String, Object[]> entry : values.entrySet()) {
            String key = entry.getKey();
            Object[] array = entry.getValue();

            double min = Double.POSITIVE_INFINITY;
            for (Object val : array) {
                if (val instanceof Number) {
                    double d = ((Number) val).doubleValue();
                    if (d < min) min = d;
                }
            }

            resultMap.put(key, min);
        }

        StatisticResult result = new StatisticResult(resultMap);
        return Stream.of(result);
    }*/









    /*private boolean skipKey(String key) {
        return key.equals("timestamps") || key.equals("name") || key.equals("start") || key.equals("end");
    }*/

    /*private double[] extractNumericValues(Object raw, int expectedLength) {
        if (raw instanceof double[] a && a.length == expectedLength) return a;
        if (raw instanceof int[] a && a.length == expectedLength)
            return Arrays.stream(a).asDoubleStream().toArray();
        if (raw instanceof long[] a && a.length == expectedLength)
            return Arrays.stream(a).asDoubleStream().toArray();
        if (raw instanceof Object[] a && a.length == expectedLength) {
            double[] d = new double[a.length];
            for (int i = 0; i < a.length; i++) {
                if (a[i] instanceof Number num) d[i] = num.doubleValue();
                else return null;
            }
            return d;
        }
        return null;
    }*/

    /*public static class StatisticResult {
        public String property;
        public String type;
        public double value;
        public String timestamp;
        public StatisticResult(String property, String type, double value, String timestamp) {
            this.property = property; this.type = type;
            this.value = value; this.timestamp = timestamp;
        }
    }*/

}
