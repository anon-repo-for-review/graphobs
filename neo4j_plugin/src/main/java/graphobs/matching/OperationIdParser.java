package graphobs.matching;

public class OperationIdParser {

    public static final String SEPARATOR = "::";

    public static String buildOperationId(String serviceName, String operationName) {
        String svc = (serviceName == null || serviceName.isBlank()) ? "unknown-service" : serviceName;
        String op = (operationName == null || operationName.isBlank()) ? "unknown-operation" : operationName;
        return svc + SEPARATOR + op;
    }

    public static String extractServiceName(String operationId) {
        if (operationId == null) return null;
        int idx = operationId.indexOf(SEPARATOR);
        return idx > 0 ? operationId.substring(0, idx) : null;
    }

    public static String extractOperationName(String operationId) {
        if (operationId == null) return null;
        int idx = operationId.indexOf(SEPARATOR);
        return idx > 0 && idx + SEPARATOR.length() < operationId.length()
                ? operationId.substring(idx + SEPARATOR.length())
                : null;
    }
}
