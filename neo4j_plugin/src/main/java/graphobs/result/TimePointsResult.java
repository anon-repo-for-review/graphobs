package graphobs.result;

import java.util.List;
import java.util.Map;

public class TimePointsResult {
    public Map<String, List<String>> values;

    public TimePointsResult(Map<String, List<String>> values) {
        this.values = values;
    }
}
