package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 *
 * @author ariane
 */
public class ConstantHistogramExpression implements TimeSeriesMetricExpression {
    private final Histogram value_;
    private final TimeSeriesMetricDeltaSet value_as_metric_;

    public ConstantHistogramExpression(Histogram value) {
        value_ = Objects.requireNonNull(value);
        value_as_metric_ = new TimeSeriesMetricDeltaSet(MetricValue.fromHistValue(value_));
    }

    public Histogram getValue() { return value_; }
    @Override
    public Collection<TimeSeriesMetricExpression> getChildren() { return Collections.EMPTY_LIST; }

    @Override
    public TimeSeriesMetricDeltaSet apply(Context ctx) {
        return value_as_metric_;
    }

    @Override
    public int getPriority() {
        return BRACKETS;
    }

    @Override
    public StringBuilder configString() {
        return new StringBuilder(getValue().toString());
    }
}
