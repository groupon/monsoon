package com.groupon.lex.metrics.json;

import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class JsonTimeSeriesValue {
    private JsonGroupName group;
    private Map<String, JsonNamedMetric> metric;

    public JsonTimeSeriesValue(TimeSeriesValue tsv) {
        this(new JsonGroupName(tsv.getGroup()), convertMetrics(tsv.getMetrics()));
    }

    private static Map<String, JsonNamedMetric> convertMetrics(Map<MetricName, MetricValue> metrics) {
        return metrics.entrySet().stream()
                .collect(Collectors.toMap(metric -> metric.getKey().configString().toString(), metric -> new JsonNamedMetric(metric.getKey(), metric.getValue())));
    }
}
