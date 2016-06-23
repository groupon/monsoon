package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.MetricMatcher;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.timeseries.TagAggregationClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricAggregate;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import java.util.Collection;
import java.util.Optional;
import lombok.Value;

/**
 *
 * @author ariane
 */
public class AvgExpression extends TimeSeriesMetricAggregate<AvgExpression.AvgAgg> {
    @Value
    public static class AvgAgg {
        private final double sum;
        private final double count;
    }

    public AvgExpression(Collection<Any2<MetricMatcher, TimeSeriesMetricExpression>> matchers, TagAggregationClause aggregation) {
        super("avg", matchers, aggregation);
    }

    @Override
    protected AvgAgg initial_() { return new AvgAgg(0, 0); }
    @Override
    protected AvgAgg map_(MetricValue x) {
        final Optional<AvgAgg> hist_params = x.histogram().map(h -> new AvgAgg(h.sum(), h.getEventCount()));
        if (hist_params.isPresent()) return hist_params.get();
        return x.value()
                .map(Number::doubleValue)
                .map(x_number -> new AvgAgg(x_number, 1))
                .orElseGet(() -> new AvgAgg(0, 0));
    }
    @Override
    protected MetricValue unmap_(AvgAgg v) {
        if (v.getCount() == 0) return MetricValue.EMPTY;
        return MetricValue.fromDblValue(v.getSum() / v.getCount());
    }
    @Override
    protected AvgAgg reducer_(AvgAgg x, AvgAgg y) {
        return new AvgAgg(x.getSum() + y.getSum(), x.getCount() + y.getCount());
    }
    @Override
    protected MetricValue scalar_fallback_() { return MetricValue.EMPTY; }

    @Override
    public int getPriority() {
        return BRACKETS;
    }
}
