package graphobs.deprecated;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.*;

public class CompareProcedure {

    /*public static class CompareSpec {
        public String labelA;
        public String labelB;
        public String relType;
        public String relLabel;
        public String start;
        public String end;
        public List<String> properties;
    }

    public static class ComparisonResult {
        public String property;
        public double difference;

        public ComparisonResult(String property, double difference) {
            this.property = property;
            this.difference = difference;
        }
    }

    @Context
    public GraphDatabaseService db;

    // @Procedure(name = "timeGraph.compare.compareBySpec", mode = Mode.READ)
    @Description("Vergleicht zwei Knotenrelationen über eine Analyseoperation ('impulse' oder 'event') und Vergleichsmetrik ('mean' oder 'median')")
    public Stream<ComparisonResult> compareBySpec(
            @Name("spec1") Map<String, Object> specMap1,
            @Name("spec2") Map<String, Object> specMap2,
            @Name("operation") String operation,
            @Name("comparison") String comparisonType) {

        CompareSpec spec1 = parseSpec(specMap1);
        CompareSpec spec2 = parseSpec(specMap2);

        List<Node> group1 = getRelatedNodes(spec1);
        List<Node> group2 = getRelatedNodes(spec2);

        Map<String, List<Double>> values1 = runOperation(group1, operation);
        Map<String, List<Double>> values2 = runOperation(group2, operation);

        Set<String> allProps = new HashSet<>(values1.keySet());
        allProps.retainAll(values2.keySet());

        return allProps.stream()
                .map(prop -> {
                    double diff = compare(values1.get(prop), values2.get(prop), comparisonType);
                    return new ComparisonResult(prop, diff);
                });
    }

    private CompareSpec parseSpec(Map<String, Object> map) {
        CompareSpec s = new CompareSpec();
        s.labelA = (String) map.get("labelA");
        s.labelB = (String) map.get("labelB");
        s.relType = (String) map.get("relType");
        s.relLabel = (String) map.get("relLabel");
        s.start = (String) map.get("start");
        s.end = (String) map.get("end");
        s.properties = (List<String>) map.get("properties");
        return s;
    }

    private List<Node> getRelatedNodes(CompareSpec spec) {
        try (Transaction tx = db.beginTx()) {
            String cypher = String.format("""
                MATCH (a:%s)-[r:%s]->(b:%s)
                WHERE r.label = $relLabel
                  AND a.time >= datetime($start) AND a.time <= datetime($end)
                RETURN b
                """, spec.labelA, spec.relType, spec.labelB);

            return tx.execute(cypher, Map.of(
                            "relLabel", spec.relLabel,
                            "start", spec.start,
                            "end", spec.end
                    )).stream()
                    .map(row -> (Node) row.get("b"))
                    .collect(Collectors.toList());
        }
    }

    private Map<String, List<Double>> runOperation(List<Node> nodes, String operation) {
        Map<String, List<Double>> resultMap = new HashMap<>();

        for (Node n : nodes) {
            switch (operation) {
                case "impulse" -> {
                    if (!n.hasProperty("peer")) continue; // Annahme: partnerknoten gespeichert
                    Node peer = (Node) n.getProperty("peer");
                    List<Double> r1 = computeVAR_IRF(n, peer);
                    mergeResult(resultMap, "impulse", r1);
                }
                case "event" -> {
                    if (!n.hasProperty("event")) continue;
                    Node event = (Node) n.getProperty("event");
                    List<EventImpactResult> impacts = runQuantifyImpact(event, n);
                    for (EventImpactResult impact : impacts) {
                        mergeResult(resultMap, impact.property, List.of(impact.peakImpact));
                    }
                }
                default -> throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        }

        return resultMap;
    }

    private void mergeResult(Map<String, List<Double>> map, String key, List<Double> values) {
        map.computeIfAbsent(key, k -> new ArrayList<>()).addAll(values);
    }

    private double compare(List<Double> a, List<Double> b, String type) {
        List<Double> diffs = new ArrayList<>();
        for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
            diffs.add(Math.abs(a.get(i) - b.get(i)));
        }

        return switch (type) {
            case "mean" -> diffs.stream().mapToDouble(d -> d).average().orElse(0.0);
            case "median" -> {
                Collections.sort(diffs);
                int s = diffs.size();
                yield s % 2 == 1
                        ? diffs.get(s / 2)
                        : (diffs.get(s / 2 - 1) + diffs.get(s / 2)) / 2.0;
            }
            default -> throw new IllegalArgumentException("Unknown comparison: " + type);
        };
    }

    private List<Double> computeVAR_IRF(Node a, Node b) {
        // Dummy-Version: rufe deine eigene logik auf oder importiere sie hier
        // return timeGraph.time.time_series.VAR_IRF(a, b, ...)
        return List.of(0.1, 0.2, 0.15); // Platzhalter
    }

    private List<EventImpactResult> runQuantifyImpact(Node event, Node ts) {
        // Dummy: Hier kann direkt der Code aus deiner QuantifyEventImpact-Procedure aufgerufen werden
        return List.of(new EventImpactResult("value1", 0.3, 5));
    }

    public static class EventImpactResult {
        public final String property;
        public final double peakImpact;
        public final long impactDuration;

        public EventImpactResult(String property, double peakImpact, long impactDuration) {
            this.property = property;
            this.peakImpact = peakImpact;
            this.impactDuration = impactDuration;
        }
    }*/
}