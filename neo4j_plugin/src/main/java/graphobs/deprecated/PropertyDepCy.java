package graphobs.deprecated;



import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

// Imports für den statistischen Test von Apache Commons Math
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.exception.NumberIsTooSmallException;

// Import für die generische Array-Verarbeitung
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Stream;



public class PropertyDepCy {

    /*@Context
    public Transaction tx;



    public static class StringResult {
        public final String value;

        public StringResult(String value) {
            this.value = value;
        }
    }

    // Füge DIESE METHODE zu deiner Klasse PropertyDependencyProcedure hinzu
    // @Procedure(name = "myplugin.helloWorld", mode = Mode.READ)
    @Description("Eine einfache Test-Prozedur, die 'Hello World' zurückgibt, um die Registrierung zu prüfen.")
    public Stream<StringResult> helloWorld() {
        return Stream.of(new StringResult("Hello World from my plugin!"));
    }



    /**
     * Ein Datencontainer für das Ergebnis der Analyse.
     * Enthält deskriptive Statistiken und die Ergebnisse des t-Tests.
     */
    /*public static class DependencyResult {
        public String property;
        public double meanGroup1;
        public double meanGroup2;
        public long countGroup1;
        public long countGroup2;
        public double absoluteDifference;
        public Double tStatistic; // Kann null sein, wenn Test nicht durchführbar
        public Double pValue;     // Kann null sein, wenn Test nicht durchführbar
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

            // Die Interpretation basiert auf dem p-Wert (Signifikanzniveau 5%)
            if (pValue == null) {
                this.interpretation = "T-Test not applicable (e.g., too few samples).";
            } else if (pValue < 0.05) {
                this.interpretation = String.format("Statistically significant difference (p=%.4f)", pValue);
            } else {
                this.interpretation = String.format("No statistically significant difference (p=%.4f)", pValue);
            }
        }
    }



    // @Procedure(name = "timeGraph.dependency_cypher.property", mode = Mode.READ)
    @Description("Vergleicht ein numerisches Property zwischen zwei Gruppen. Die Grundgesamtheit wird durch 'commonSpec' definiert, die erste Gruppe durch einen Cypher-Bedingungsstring ('group1ConditionCypher').")
    public Stream<DependencyResult> analyzePropertyDependency(
            @Name("commonSpec") Map<String, Object> commonSpec,
            @Name("group1ConditionCypher") String group1ConditionCypher
    ) {
        // --- 1. Input-Validierung ---
        if (!commonSpec.containsKey("targetProperty") || !(commonSpec.get("targetProperty") instanceof String)) {
            throw new IllegalArgumentException("Der Parameter 'commonSpec' muss einen Schlüssel 'targetProperty' mit einem String-Wert enthalten.");
        }
        String targetProperty = (String) commonSpec.get("targetProperty");

        if (group1ConditionCypher == null || group1ConditionCypher.trim().isEmpty()) {
            throw new IllegalArgumentException("Der Parameter 'group1ConditionCypher' darf nicht leer sein.");
        }

        List<Double> values1 = new ArrayList<>();
        List<Double> values2 = new ArrayList<>();
        Map<String, Object> params = new HashMap<>();

        // --- 2. Effiziente Abfrage erstellen und ausführen ---
        String combinedQuery = buildCombinedQuery(commonSpec, group1ConditionCypher, params);

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

        // --- 3. Statistiken berechnen und t-Test durchführen ---
        long count1 = values1.size();
        double sum1 = values1.stream().mapToDouble(Double::doubleValue).sum();
        double mean1 = count1 > 0 ? sum1 / count1 : 0;

        long count2 = values2.size();
        double sum2 = values2.stream().mapToDouble(Double::doubleValue).sum();
        double mean2 = count2 > 0 ? sum2 / count2 : 0;

        Double tValue = null;
        Double pValue = null;

        // Ein t-Test benötigt mindestens 2 Werte pro Gruppe, um sinnvoll zu sein.
        if (count1 >= 2 && count2 >= 2) {
            try {
                TTest tTest = new TTest();
                double[] sample1 = values1.stream().mapToDouble(d -> d).toArray();
                double[] sample2 = values2.stream().mapToDouble(d -> d).toArray();

                tValue = tTest.t(sample1, sample2);
                pValue = tTest.tTest(sample1, sample2);
            } catch (NumberIsTooSmallException e) {
                // Fängt Fehler ab, z.B. wenn eine Stichprobe keine Varianz hat.
                // In diesem Fall bleiben tValue und pValue null, was korrekt behandelt wird.
            }
        }

        return Stream.of(new DependencyResult(targetProperty, mean1, mean2, count1, count2, tValue, pValue));
    }

    /**
     * Erstellt eine einzige Cypher-Abfrage, die Knoten auf Basis einer Cypher-Bedingung in zwei Gruppen aufteilt.
     * ACHTUNG: Der Parameter 'group1ConditionCypher' wird direkt in die Abfrage eingefügt.
     * Dies stellt ein Sicherheitsrisiko (Cypher Injection) dar, wenn der String von einer nicht vertrauenswürdigen Quelle stammt.
     */
    /*private String buildCombinedQuery(Map<String, Object> commonSpec, String group1ConditionCypher, Map<String, Object> params) {
        StringBuilder query = new StringBuilder("MATCH (n");

        // Gemeinsame Spezifikationen (Label, Relation, Properties)
        if (commonSpec.containsKey("label")) {
            // Label-Namen können nicht parametrisiert werden
            query.append(":").append(commonSpec.get("label"));
        }
        query.append(")");

        if (commonSpec.containsKey("relation")) {
            // Beziehungstypen können nicht parametrisiert werden
            query.append("-[:").append(commonSpec.get("relation")).append("]->()");
        }

        List<String> whereClauses = new ArrayList<>();
        int paramCounter = 0;

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

        // Partitionierung in Gruppen mithilfe der übergebenen Cypher-Bedingung.
        // Der Knoten, auf den sich die Bedingung beziehen muss, heißt immer 'n'.
        query.append(" WITH n, CASE WHEN (").append(group1ConditionCypher).append(") THEN 1 ELSE 2 END AS groupId");
        query.append(" RETURN n, groupId");

        return query.toString();
    }

    /**
     * Extrahiert einen einzelnen numerischen Wert aus einer Property.
     * - Wenn die Property eine Zahl ist, wird sie zurückgegeben.
     * - Wenn die Property ein Array von Zahlen ist, wird deren Durchschnitt berechnet und zurückgegeben.
     * - In allen anderen Fällen (z.B. String, leeres Array, null) wird Optional.empty() zurückgegeben.
     */
    /*private Optional<Double> getNumericValueFromProperty(Object propertyValue) {
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
