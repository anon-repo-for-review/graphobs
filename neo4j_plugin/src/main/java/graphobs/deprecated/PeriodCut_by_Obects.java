package graphobs.deprecated;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class PeriodCut_by_Obects {

    @Context
    public GraphDatabaseService db;

    // @Procedure(name = "timeGraph.time.time_series.PeriodCut_by_Object", mode = Mode.READ)
    @Description("Filtert eine time_series basierend auf einem Zeitraum-Knoten (time_period oder time_series)")
    public Stream<FilteredTimeSeriesResult> filterTimeSeriesByRangeObject(
            @Name("rangeObject") Node rangeObject,
            @Name("timeSeries") Node timeSeriesNode) {

        if (!rangeObject.hasProperty("start") || !rangeObject.hasProperty("end")) {
            throw new IllegalArgumentException("rangeObject muss 'start' und 'end' Properties enthalten.");
        }

        ZonedDateTime start = parseToZonedDateTime(rangeObject.getProperty("start"));
        ZonedDateTime end = parseToZonedDateTime(rangeObject.getProperty("end"));


        // Bereichsprüfung gegen timeSeriesNode.start und end
        if (timeSeriesNode.hasProperty("start") && timeSeriesNode.hasProperty("end")) {
            ZonedDateTime seriesStart = parseToZonedDateTime(timeSeriesNode.getProperty("start"));
            ZonedDateTime seriesEnd = parseToZonedDateTime(timeSeriesNode.getProperty("end"));

            if (end.isBefore(seriesStart) || start.isAfter(seriesEnd)) {
                return Stream.empty();
            }
        }

        return filterTimeSeries(timeSeriesNode, start, end);
    }

    private Stream<FilteredTimeSeriesResult> filterTimeSeries(Node tsNode, ZonedDateTime start, ZonedDateTime end) {
        String[] timestamps = (String[]) tsNode.getProperty("timestamps", new String[0]);
        Map<String, Object[]> valueSeries = extractAlignedValueProperties(tsNode, timestamps.length);

        List<FilteredTimeSeriesResult> resultList = new ArrayList<>();

        for (int i = 0; i < timestamps.length; i++) {
            ZonedDateTime ts = parseToZonedDateTime(timestamps[i]);
            if (!ts.isBefore(start) && !ts.isAfter(end)) {
                Map<String, Object> rowValues = new HashMap<>();
                for (Map.Entry<String, Object[]> entry : valueSeries.entrySet()) {
                    rowValues.put(entry.getKey(), entry.getValue()[i]);
                }
                resultList.add(new FilteredTimeSeriesResult(i, ts.toString(), rowValues));
            }
        }

        return resultList.stream();
    }

    private Map<String, Object[]> extractAlignedValueProperties(Node node, int expectedLength) {
        Map<String, Object[]> result = new HashMap<>();
        for (String key : node.getPropertyKeys()) {
            if (key.equals("timestamps")) continue;

            Object raw = node.getProperty(key);
            Object[] array = null;

            if (raw instanceof Object[]) {
                array = (Object[]) raw;
            } else if (raw instanceof int[]) {
                array = Arrays.stream((int[]) raw).boxed().toArray();
            } else if (raw instanceof long[]) {
                array = Arrays.stream((long[]) raw).boxed().toArray();
            } else if (raw instanceof double[]) {
                array = Arrays.stream((double[]) raw).boxed().toArray();
            }

            if (array != null && array.length == expectedLength) {
                result.put(key, array);
            }
        }
        return result;
    }

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