package graphobs.datasources.prometheus;

import org.neo4j.logging.Log;
import java.time.Instant;
import java.util.*;

public class PrometheusRegistrationService {
    private final PrometheusClient client;
    private final PodRepository repo;
    private final Log log;

    public PrometheusRegistrationService(PrometheusClient client, PodRepository repo, Log log) {
        this.client = client;
        this.repo = repo;
        this.log = log;
    }

    public long register(String prometheusUrl) throws Exception {
        // 1) metrics
        List<String> metricNames = client.fetchMetricNames();

        // 2) discover pods (need podLabel before creating Prometheus node)
        PrometheusClient.LabelValuesResult labelRes = client.fetchFirstAvailableLabelValuesAndLabel();
        List<String> podNames = labelRes.values;
        String podLabel = labelRes.label; // discovered pod label name (e.g. "pod", "pod_name")

        // 3) discover servers (need serverLabel before creating Prometheus node)
        PrometheusClient.ServerNamesResult serverRes = client.fetchServerNamesWithLabel();
        List<String> serverNames = serverRes.names;
        String serverLabel = serverRes.label; // discovered server label name (e.g. "node", "instance")

        // 4) ensure prometheus node with discovered label metadata
        repo.ensurePrometheusNode(prometheusUrl, metricNames, podLabel, serverLabel);

        // 5) build upsert rows for pods (batch: 3 queries instead of N*3)
        Map<String, PrometheusClient.TimeRange> podRanges = client.getBatchPodTimeRanges(podNames, podLabel);
        List<Map<String,Object>> rows = new ArrayList<>(podNames.size());
        for (String p : podNames) {
            PrometheusClient.TimeRange range = podRanges.getOrDefault(p, new PrometheusClient.TimeRange(null, null));
            Map<String,Object> m = new HashMap<>();
            m.put("name", p);
            m.put("start", range.start);
            m.put("end", range.end);
            m.put("status", "RUNNING");
            rows.add(m);
        }

        // 6) write pods (bulk) and link to prometheus
        repo.upsertPodsBulk(rows, prometheusUrl);
        repo.linkPodsToPrometheus(prometheusUrl);


        // --- B) SERVERS ---
        Map<String, PrometheusClient.TimeRange> serverRanges = client.getBatchServerTimeRanges(serverNames);
        List<Map<String, Object>> serverRows = new ArrayList<>(serverNames.size());
        for (String s : serverNames) {
            PrometheusClient.TimeRange range = serverRanges.getOrDefault(s, new PrometheusClient.TimeRange(null, null));
            Map<String, Object> m = new HashMap<>();
            m.put("name", s);
            m.put("start", range.start);
            m.put("end", range.end);
            m.put("status", "RUNNING");
            serverRows.add(m);
        }
        repo.upsertServersBulk(serverRows, prometheusUrl);

        // --- C) LINKING (NEU) ---
        // Pods mit Servern verknüpfen
        Map<String, String> podToServer = client.fetchPodToServerMapping();
        repo.linkPodsToServers(podToServer);

        // 6) optionally start updater (caller handles scheduling) — return pod count
        return podNames.size();
    }
}
