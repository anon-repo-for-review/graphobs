package graphobs.query.timeseries.aggregations;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import graphobs.result.TimeSeriesResult;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.*;

//import static graphobs.query.timeseries.TimeSeriesUtil.getValueProperties;

public class MovingAverage {

    @Context
    public GraphDatabaseService db;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    // === Moving Average by Window Size (number of points) ===
    /*@Procedure(name = "decrapted.aggregation.moving_average", mode = Mode.READ)
    public Stream<TimeSeriesResult> movingAverageByPoints(
            @Name("timeSeries") Node timeSeriesNode,
            @Name("windowSize") long windowSize
            //@Name(value = "property", defaultValue = "") String property
    ) {
        String[] timestamps = (String[]) timeSeriesNode.getProperty("timestamps", new String[0]);
        int length = timestamps.length;

        Map<String, Object[]> valuesMap = getValueProperties(timeSeriesNode, length);



        Map<String, List<Double>> resultValues = new HashMap<>();
        List<String> resultTimestamps = new ArrayList<>();

        for (Map.Entry<String, Object[]> entry : valuesMap.entrySet()) {
            String key = entry.getKey();
            Object[] values = entry.getValue();
            List<Double> movingAvg = new ArrayList<>();

            for (int i = 0; i <= values.length - windowSize; i++) {
                double sum = 0;
                for (int j = 0; j < windowSize; j++) {
                    sum += ((Number) values[i + j]).doubleValue();
                }
                movingAvg.add(sum / windowSize);
                if (resultTimestamps.size() < movingAvg.size()) {
                    resultTimestamps.add(timestamps[(int) (i + windowSize - 1)]);
                }
            }
            resultValues.put(key, movingAvg);
        }

        return Stream.of(new TimeSeriesResult(resultTimestamps, resultValues));
    }*/

    // === Moving Average by Time Duration ===
    /*@Procedure(name = "timeGraph.time.time_series.moving_average_by_time_old", mode = Mode.READ)
    public Stream<MovingAverageResult> movingAverageBySeconds(
            @Name("timeSeries") Node timeSeriesNode,
            @Name("seconds") long seconds,
            @Name(value = "property", defaultValue = "") String property
    ) {
        String[] timestamps = (String[]) timeSeriesNode.getProperty("timestamps", new String[0]);
        int length = timestamps.length;
        ZonedDateTime[] timeObjs = Arrays.stream(timestamps)
                .map(this::parseToZonedDateTime)
                .toArray(ZonedDateTime[]::new);

        Map<String, Object[]> valuesMap = getValueProperties(timeSeriesNode, length);

        if (!property.isEmpty()) {
            valuesMap.keySet().retainAll(Collections.singleton(property));
        }

        Map<String, List<Double>> resultValues = new HashMap<>();
        List<String> resultTimestamps = new ArrayList<>();

        for (Map.Entry<String, Object[]> entry : valuesMap.entrySet()) {
            String key = entry.getKey();
            Object[] values = entry.getValue();
            List<Double> movingAvg = new ArrayList<>();

            for (int i = 0; i < length; i++) {
                ZonedDateTime start = timeObjs[i];
                ZonedDateTime end = start.plusSeconds(seconds);
                int count = 0;
                double sum = 0;

                for (int j = i; j < length; j++) {
                    if (timeObjs[j].isAfter(end)) break;
                    sum += ((Number) values[j]).doubleValue();
                    count++;
                }

                if (count > 0) {
                    movingAvg.add(sum / count);
                    resultTimestamps.add(timestamps[i]);
                }
            }

            resultValues.put(key, movingAvg);
        }

        return Stream.of(new MovingAverageResult(resultTimestamps, resultValues));
    }*/

    // === Helper: Get only valid arrays ===
    /*private Map<String, Object[]> getValueProperties(Node node, int expectedLength) {
        Map<String, Object[]> result = new HashMap<>();
        for (String key : node.getPropertyKeys()) {
            if (key.equals("timestamps") || key.equals("name")) continue;

            Object raw = node.getProperty(key);
            if (!raw.getClass().isArray()) continue;

            Object[] array = convertToObjectArray(raw);
            if (array.length == expectedLength) {
                result.put(key, array);
            }
        }
        return result;
    }

    private Object[] convertToObjectArray(Object raw) {
        if (raw instanceof Object[]) return (Object[]) raw;
        if (raw instanceof int[]) return Arrays.stream((int[]) raw).boxed().toArray();
        if (raw instanceof long[]) return Arrays.stream((long[]) raw).boxed().toArray();
        if (raw instanceof double[]) return Arrays.stream((double[]) raw).boxed().toArray();
        throw new IllegalArgumentException("Unsupported array type: " + raw.getClass().getName());
    }

    private ZonedDateTime parseToZonedDateTime(Object value) {
        if (value instanceof ZonedDateTime) return (ZonedDateTime) value;
        if (value instanceof String) return ZonedDateTime.parse((String) value, FORMATTER);
        if (value instanceof Long) return Instant.ofEpochMilli((Long) value).atZone(ZoneId.systemDefault());
        throw new IllegalArgumentException("Unsupported date format: " + value);
    }

    // === Result DTO ===
    public static class MovingAverageResult {
        public List<String> timestamps;
        public Map<String, List<Double>> values;

        public MovingAverageResult(List<String> timestamps, Map<String, List<Double>> values) {
            this.timestamps = timestamps;
            this.values = values;
        }
    }*/


    @Procedure(name = "graphobs.aggregation.moving_average", mode = Mode.READ)
    public Stream<TimeSeriesResult> movingAverageByPoints(
            @Name("timestamps") List<String> timestamps,
            @Name("values") Map<String, List<Double>> values,
            @Name(value = "window", defaultValue = "5") long w_size
    ) {
        TimeSeriesResult timeSeriesResult = calc_moving_average(timestamps.toArray(new String[0]), values, w_size);
        return Stream.of(timeSeriesResult);
    }


    public static TimeSeriesResult calc_moving_average(String[] timestampStrs, Map<String, List<Double>> valueMap, long windowSize){
        Map<String, List<Double>> resultValues = new HashMap<>();
        List<String> resultTimestamps = new ArrayList<>();

        for (Map.Entry<String, List<Double>> entry : valueMap.entrySet()) {
            String key = entry.getKey();
            List<Double> values = entry.getValue();
            List<Double> movingAvg = new ArrayList<>();

            for (int i = 0; i <= values.size() - windowSize; i++) {
                double sum = 0;
                for (int j = 0; j < windowSize; j++) {
                    sum += values.get(i + j);
                }
                movingAvg.add(sum / windowSize);
                if (resultTimestamps.size() < movingAvg.size()) {
                    resultTimestamps.add(timestampStrs[(int) (i + windowSize - 1)]);
                }
            }
            resultValues.put(key, movingAvg);
        }

        return new TimeSeriesResult(resultTimestamps, resultValues);
    }
}