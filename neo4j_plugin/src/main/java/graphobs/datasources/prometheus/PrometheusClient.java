package graphobs.datasources.prometheus;

import graphobs.matching.FieldNameResolver;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;


public interface PrometheusClient {

    String[] POD_LABEL_CANDIDATES = FieldNameResolver.PROMETHEUS_POD_LABELS;

    class LabelValuesResult {
        public final String label;
        public final List<String> values;
        public LabelValuesResult(String label, List<String> values) { this.label = label; this.values = values; }
    }

    class ServerNamesResult {
        public final String label;
        public final List<String> names;
        public ServerNamesResult(String label, List<String> names) { this.label = label; this.names = names; }
    }

    class TimeRange {
        // Using ZonedDateTime (UTC) instead of ISO strings for consistency
        public final ZonedDateTime start;
        public final ZonedDateTime end;
        public TimeRange(ZonedDateTime start, ZonedDateTime end) { this.start = start; this.end = end; }
    }

    ServerNamesResult fetchServerNamesWithLabel() throws Exception;

    // NEU: Zeitspanne für Server ermitteln (ähnlich wie Pods)
    TimeRange getServerTimeRange(String serverName) throws Exception;

    // NEU: Mapping Map<PodName, ServerName>
    Map<String, String> fetchPodToServerMapping() throws Exception;

    List<String> fetchMetricNames() throws Exception;
    LabelValuesResult fetchFirstAvailableLabelValuesAndLabel() throws Exception;
    TimeRange getPodTimeRange(String podName, String genericLabel) throws Exception;
    ZonedDateTime getPodLastSeenAsZdt(String label, String podName) throws Exception;
    long getPrometheusTime() throws Exception;

    /** Batch: fetches TimeRanges for ALL pods in 3 queries instead of N*3. */
    Map<String, TimeRange> getBatchPodTimeRanges(List<String> podNames, String genericLabel) throws Exception;

    /** Batch: fetches lastSeen for ALL pods in 1 query instead of N. */
    Map<String, ZonedDateTime> getBatchPodLastSeen(String label, List<String> podNames) throws Exception;

    /** Batch: fetches TimeRanges for ALL servers in 2 queries instead of N*3. */
    Map<String, TimeRange> getBatchServerTimeRanges(List<String> serverNames) throws Exception;
}
