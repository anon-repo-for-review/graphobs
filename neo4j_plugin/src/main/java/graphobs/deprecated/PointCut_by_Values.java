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

public class PointCut_by_Values {

    @Context
    public GraphDatabaseService db;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public static class NodeResult {
        public final Node node;

        public NodeResult(Node node) {
            this.node = node;
        }
    }

    // @Procedure(name = "timeGraph.time.time_series.PointCut_by_Values", mode = Mode.READ)
    public Stream<TimeSeriesSliceResult> pointCutByValues(
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
            case "plus" -> {
                start = reference;
                end = reference.plus(duration);
            }
            case "minus" -> {
                start = reference.minus(duration);
                end = reference;
            }
            case "both", "around" -> {
                start = reference.minus(duration);
                end = reference.plus(duration);
            }
            default -> throw new IllegalArgumentException("Ungültiger direction-Wert: " + direction);
        }

        if (timeSeriesNode.hasProperty("start") && timeSeriesNode.hasProperty("end")) {
            ZonedDateTime seriesStart = parseToZonedDateTime(timeSeriesNode.getProperty("start"));
            ZonedDateTime seriesEnd = parseToZonedDateTime(timeSeriesNode.getProperty("end"));

            if (end.isBefore(seriesStart) || start.isAfter(seriesEnd)) {
                return Stream.empty();
            }
        }



        // Hole Zeitstempel
        Object[] rawTimestamps = (Object[]) timeSeriesNode.getProperty("timestamps");
        List<Long> matchingIndices = new ArrayList<>();
        List<String> filteredTimestamps = new ArrayList<>();
        String name = "no name";

        for (int i = 0; i < rawTimestamps.length; i++) {
            ZonedDateTime ts = parseToZonedDateTime(rawTimestamps[i]);
            if (!ts.isBefore(start) && !ts.isAfter(end)) {
                matchingIndices.add((long) i);
                filteredTimestamps.add(ts.toString());
            }
        }

        // Werte sammeln
        Map<String, List<Object>> resultValues = new HashMap<>();
        for (String key : timeSeriesNode.getPropertyKeys()) {
            if (key.equals("name")){
                name = timeSeriesNode.getProperty("name").toString();
            }

            if (key.equals("timestamps") || key.equals("name")) continue;



            Object raw = timeSeriesNode.getProperty(key);
            Object[] values = null;
            if (raw instanceof Object[]) values = (Object[]) raw;
            else if (raw instanceof int[]) values = Arrays.stream((int[]) raw).boxed().toArray();
            else if (raw instanceof long[]) values = Arrays.stream((long[]) raw).boxed().toArray();
            else if (raw instanceof double[]) values = Arrays.stream((double[]) raw).boxed().toArray();

            if (values != null && values.length == rawTimestamps.length) {
                List<Object> filtered = new ArrayList<>();
                for (long i : matchingIndices) {
                    filtered.add(values[(int) i]);
                }
                resultValues.put(key, filtered);
            }
        }

        return Stream.of(new TimeSeriesSliceResult(matchingIndices, filteredTimestamps, resultValues, name));
    }


    public static class TimeSeriesSliceResult {
        public List<Long> index;
        public List<String> timestamps;
        public Map<String, List<Object>> values;
        public String name;

        public TimeSeriesSliceResult(List<Long> index, List<String> timestamps, Map<String, List<Object>> values, String name) {
            this.index = index;
            this.timestamps = timestamps;
            this.values = values;
            this.name = name;
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
