package graphobs.deprecated;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.exception.NumberIsTooSmallException;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Stream;

public class PropertyDependencyProcedure {

    /*public static class DependencyResult {
        // ... (unverändert von der vorherigen Version)
        public String property;
        public double meanGroup1;
        public double meanGroup2;
        public long countGroup1;
        public long countGroup2;
        public double absoluteDifference;
        public Double tStatistic;
        public Double pValue;
        public String interpretation;

        public DependencyResult(String property, double mean1, double mean2, long count1, long count2, Double t, Double p) {
            this.property = property;
            this.meanGroup1 = mean1;
            this.meanGroup2 = mean2;
            this.countGroup1 = count1;
            this.countGroup2 = count2;
            this.absoluteDifference = Math.abs(mean1 - mean2);
            this.tStatistic = t;
            this.pValue = p;

            if (pValue == null) {
                this.interpretation = "T-Test not applicable (e.g., too few samples).";
            } else if (pValue < 0.05) {
                this.interpretation = String.format("Statistically significant difference (p=%.4f)", pValue);
            } else {
                this.interpretation = String.format("No statistically significant difference (p=%.4f)", pValue);
            }
        }
    }

    @Context
    public Transaction tx;

    // @Procedure(name = "timeGraph.analyzePropertyDependency", mode = Mode.READ)
    @Description("Vergleicht ein numerisches Property zwischen zwei Gruppen. Die Grundgesamtheit wird durch 'commonSpec' definiert, die erste Gruppe durch 'group1Spec'. Die zweite Gruppe ist der Rest der Grundgesamtheit.")
    public Stream<DependencyResult> analyzePropertyDependency(
            @Name("commonSpec") Map<String, Object> commonSpec,
            @Name("group1Spec") Map<String, Object> group1Spec
    ) {
        // --- 1. Input-Validierung ---
        if (!commonSpec.containsKey("targetProperty") || !(commonSpec.get("targetProperty") instanceof String)) {
            throw new IllegalArgumentException("Der Parameter 'commonSpec' muss einen Schlüssel 'targetProperty' mit einem String-Wert enthalten.");
        }
        String targetProperty = (String) commonSpec.get("targetProperty");

        List<Double> values1 = new ArrayList<>();
        List<Double> values2 = new ArrayList<>();
        Map<String, Object> params = new HashMap<>();

        // --- 2. Effiziente Abfrage erstellen und ausführen ---
        String combinedQuery = buildCombinedQuery(commonSpec, group1Spec, params);

        try (Result result = tx.execute(combinedQuery, params)) {
            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                Node n = (Node) row.get("n");
                long groupId = (long) row.get("groupId"); // Cypher CASE gibt Long zurück

                if (n.hasProperty(targetProperty)) {
                    Object rawValue = n.getProperty(targetProperty);
                    Optional<Double> numericValue = getNumericValueFromProperty(rawValue);

                    if (numericValue.isPresent()) {
                        if (groupId == 1) {
                            values1.add(numericValue.get());
                        } else {
                            values2.add(numericValue.get());
                        }
                    }
                }
            }
        }

        // --- 3. Statistiken berechnen und t-Test durchführen (unverändert) ---
        long count1 = values1.size();
        double sum1 = values1.stream().mapToDouble(Double::doubleValue).sum();
        double mean1 = count1 > 0 ? sum1 / count1 : 0;

        long count2 = values2.size();
        double sum2 = values2.stream().mapToDouble(Double::doubleValue).sum();
        double mean2 = count2 > 0 ? sum2 / count2 : 0;

        Double tValue = null;
        Double pValue = null;

        if (count1 >= 2 && count2 >= 2) {
            try {
                TTest tTest = new TTest();
                double[] sample1 = values1.stream().mapToDouble(d -> d).toArray();
                double[] sample2 = values2.stream().mapToDouble(d -> d).toArray();
                tValue = tTest.t(sample1, sample2);
                pValue = tTest.tTest(sample1, sample2);
            } catch ( NumberIsTooSmallException e) {
                // Fehler abfangen
            }
        }

        return Stream.of(new DependencyResult(targetProperty, mean1, mean2, count1, count2, tValue, pValue));
    }

    /**
     * Erstellt eine einzige Cypher-Abfrage, die Knoten in zwei Gruppen aufteilt.
     * Gruppe 1 erfüllt die group1Spec, Gruppe 2 ist der Rest.
     */
    /*private String buildCombinedQuery(Map<String, Object> commonSpec, Map<String, Object> group1Spec, Map<String, Object> params) {
        StringBuilder query = new StringBuilder("MATCH (n");

        // Gemeinsames Label
        if (commonSpec.containsKey("label")) {
            query.append(":").append(commonSpec.get("label"));
        }
        query.append(")");

        // Gemeinsame Beziehung
        if (commonSpec.containsKey("relation")) {
            query.append("-[:").append(commonSpec.get("relation")).append("]->()");
        }

        List<String> whereClauses = new ArrayList<>();
        int paramCounter = 0;

        // Gemeinsame Properties
        if (commonSpec.containsKey("property")) {
            Map<String, Object> props = (Map<String, Object>) commonSpec.get("property");
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                String paramName = "common_p" + paramCounter++;
                whereClauses.add("n." + entry.getKey() + " = $" + paramName);
                params.put(paramName, entry.getValue());
            }
        }

        if (!whereClauses.isEmpty()) {
            query.append(" WHERE ").append(String.join(" AND ", whereClauses));
        }

        // Partitionierung in Gruppen mithilfe einer CASE-Anweisung
        query.append(" WITH n, CASE WHEN ");

        List<String> group1Conditions = new ArrayList<>();
        if (group1Spec.containsKey("property")) {
            Map<String, Object> props = (Map<String, Object>) group1Spec.get("property");
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                String paramName = "g1_p" + paramCounter++;
                group1Conditions.add("n." + entry.getKey() + " = $" + paramName);
                params.put(paramName, entry.getValue());
            }
        }

        if (group1Conditions.isEmpty()) {
            // Falls group1Spec leer ist, gibt es technisch keine Gruppe 1
            query.append("false");
        } else {
            query.append(String.join(" AND ", group1Conditions));
        }

        query.append(" THEN 1 ELSE 2 END AS groupId");
        query.append(" RETURN n, groupId");

        return query.toString();
    }

    private Optional<Double> getNumericValueFromProperty(Object propertyValue) {
        if (propertyValue instanceof Number) {
            return Optional.of(((Number) propertyValue).doubleValue());
        }

        if (propertyValue != null && propertyValue.getClass().isArray()) {
            List<Double> numbersInArray = new ArrayList<>();
            int length = Array.getLength(propertyValue);
            for (int i = 0; i < length; i++) {
                Object element = Array.get(propertyValue, i);
                if (element instanceof Number) {
                    numbersInArray.add(((Number) element).doubleValue());
                }
            }

            if (numbersInArray.isEmpty()) {
                return Optional.empty(); // Leeres Array oder Array ohne Zahlen hat keinen Durchschnitt
            }

            double sum = numbersInArray.stream().mapToDouble(Double::doubleValue).sum();
            return Optional.of(sum / numbersInArray.size());
        }

        return Optional.empty();
    }*/

}