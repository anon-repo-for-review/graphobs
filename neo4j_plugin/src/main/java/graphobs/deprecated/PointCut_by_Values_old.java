package graphobs.deprecated;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

public class PointCut_by_Values_old {

    @Context
    public GraphDatabaseService db;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public static class NodeResult {
        public final Node node;

        public NodeResult(Node node) {
            this.node = node;
        }
    }

    // @Procedure(name = "timeGraph.time.time_series.PointCut_by_Values_old", mode = Mode.READ)
    public Stream<FilteredTimeSeriesResult> pointCutByValues(
            @Name("referenceTime") Object referenceTime,
            @Name("duration") String durationStr,
            @Name("direction") String direction,
            @Name("timeSeries") Node timeSeriesNode) {

        if (!timeSeriesNode.hasLabel(Label.label("time_series")) || !timeSeriesNode.hasProperty("timestamps")) {
            throw new IllegalArgumentException("timeSeries muss ein 'time_series'-Knoten mit 'timestamps'-Property sein.");
        }

        ZonedDateTime reference = parseToZonedDateTime(referenceTime);
        Duration duration = Duration.parse(durationStr);
        ZonedDateTime start, end;

        switch (direction) {
            case "plus":
                start = reference;
                end = reference.plus(duration);
                break;
            case "minus":
                start = reference.minus(duration);
                end = reference;
                break;
            case "both":
                start = reference.minus(duration);
                end = reference.plus(duration);
                break;
            default:
                throw new IllegalArgumentException("Ungültiger direction-Wert: " + direction);
        }

        return filterTimeSeries(timeSeriesNode, start, end);
    }


    /*private Stream<FilteredTimeSeriesResult> filterTimeSeries(Node timeSeriesNode, ZonedDateTime start, ZonedDateTime end) {
        String[] timestamps = (String[]) timeSeriesNode.getProperty("timestamps", new String[0]);
        Object[] values = extractValues(timeSeriesNode);

        List<FilteredTimeSeriesResult> resultList = new ArrayList<>();
        for (int i = 0; i < timestamps.length; i++) {
            ZonedDateTime ts = parseToZonedDateTime(timestamps[i]);
            if (!ts.isBefore(start) && !ts.isAfter(end)) {
                Object value = (values != null && i < values.length) ? values[i] : null;
                resultList.add(new FilteredTimeSeriesResult(i, ts.toString(), value));
            }
        }

        return resultList.stream();
    }*/


    private Stream<FilteredTimeSeriesResult> filterTimeSeries(Node timeSeriesNode, ZonedDateTime start, ZonedDateTime end) {
        Object[] rawTimestamps = (Object[]) timeSeriesNode.getProperty("timestamps", new Object[0]);
        ZonedDateTime[] timestamps = Arrays.stream(rawTimestamps)
                .map(this::parseToZonedDateTime)
                .toArray(ZonedDateTime[]::new);

        Map<String, Object[]> valueColumns = extractMatchingValueColumns(timeSeriesNode, timestamps.length);

        List<FilteredTimeSeriesResult> results = new ArrayList<>();
        for (int i = 0; i < timestamps.length; i++) {
            ZonedDateTime ts = timestamps[i];
            if (!ts.isBefore(start) && !ts.isAfter(end)) {
                Map<String, Object> valuesAtIndex = new HashMap<>();
                for (Map.Entry<String, Object[]> entry : valueColumns.entrySet()) {
                    valuesAtIndex.put(entry.getKey(), entry.getValue()[i]);
                }
                results.add(new FilteredTimeSeriesResult(i, ts.toString(), valuesAtIndex));
            }
        }

        return results.stream();
    }



    /*private Object[] extractValues(Node node) {
        if (!node.hasProperty("values")) return null;

        Object raw = node.getProperty("values");
        if (raw instanceof Object[]) return (Object[]) raw;
        if (raw instanceof int[]) return Arrays.stream((int[]) raw).boxed().toArray();
        if (raw instanceof long[]) return Arrays.stream((long[]) raw).boxed().toArray();
        if (raw instanceof double[]) return Arrays.stream((double[]) raw).boxed().toArray();
        throw new IllegalArgumentException("Nicht unterstützter Typ für 'values'.");
    }*/

    private Map<String, Object[]> extractMatchingValueColumns(Node node, int expectedLength) {
        Map<String, Object[]> result = new HashMap<>();

        for (String key : node.getPropertyKeys()) {
            if (key.equals("timestamps")) continue;  // Timestamps selbst überspringen

            Object prop = node.getProperty(key);
            Object[] valuesArray = null;

            if (prop instanceof Object[] arr && arr.length == expectedLength) {
                valuesArray = arr;
            } else if (prop instanceof int[] arr && arr.length == expectedLength) {
                valuesArray = Arrays.stream(arr).boxed().toArray(Integer[]::new);
            } else if (prop instanceof long[] arr && arr.length == expectedLength) {
                valuesArray = Arrays.stream(arr).boxed().toArray(Long[]::new);
            } else if (prop instanceof double[] arr && arr.length == expectedLength) {
                valuesArray = Arrays.stream(arr).boxed().toArray(Double[]::new);
            }

            if (valuesArray != null) {
                result.put(key, valuesArray);
            }
        }

        return result;
    }


    /*public static class FilteredTimeSeriesResult {
        public long index;
        public String timestamp;
        public Object value;

        public FilteredTimeSeriesResult(long index, String timestamp, Object value) {
            this.index = index;
            this.timestamp = timestamp;
            this.value = value;
        }
    }*/


    public static class FilteredTimeSeriesResult {
        public long index;
        public String timestamp;
        public Map<String, Object> values;

        public FilteredTimeSeriesResult(long index, String timestamp, Map<String, Object> values) {
            this.index = index;
            this.timestamp = timestamp;
            this.values = values;
        }
    }







    private ZonedDateTime parseToZonedDateTime(Object value) {
        if (value instanceof ZonedDateTime) {
            return (ZonedDateTime) value;
        } else if (value instanceof String) {
            return ZonedDateTime.parse((String) value, DateTimeFormatter.ISO_DATE_TIME);
        } else if (value instanceof Long) {
            return Instant.ofEpochMilli((Long) value).atZone(ZoneId.systemDefault());
        } else {
            throw new IllegalArgumentException("Unsupported date format: " + value);
        }
    }



}