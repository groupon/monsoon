package com.groupon.lex.metrics.json;

import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class JsonNamedMetric {
    private List<String> name;
    private Object value;

    public JsonNamedMetric(MetricName name, MetricValue value) {
        this(name.getPath(), Json.extractMetricValue(value));
    }
}
