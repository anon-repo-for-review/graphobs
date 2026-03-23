package graphobs.matching;


// eventuell für Erweiterung noch nützlich

public class EntityNameNormalizer {

    /**
     * Normalizes a pod name from any source (Prometheus, Jaeger, OpenSearch).
     * Removes namespace prefixes like "default/pod-1" and trims whitespace.
     */
    /*public static String normalizePodName(String raw) {
        if (raw == null) return null;
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) return null;
        // Remove namespace prefix: "default/pod-1" → "pod-1"
        int slashIdx = trimmed.indexOf('/');
        if (slashIdx >= 0 && slashIdx < trimmed.length() - 1) {
            trimmed = trimmed.substring(slashIdx + 1);
        }
        return trimmed;
    }*/

    /**
     * Normalizes a service name. Trims whitespace.
     */
    /*public static String normalizeServiceName(String raw) {
        if (raw == null) return null;
        String trimmed = raw.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }*/

    /**
     * Normalizes a server/instance name. Trims whitespace.
     */
    /*public static String normalizeServerName(String raw) {
        if (raw == null) return null;
        String trimmed = raw.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }*/
}
