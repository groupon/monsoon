package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricMatcher;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.timeseries.TagAggregationClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricAggregate;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import java.util.Collection;
import java.util.Optional;

/**
 *
 * @author ariane
 */
public class MinExpression extends TimeSeriesMetricAggregate<Optional<Number>> {
    public MinExpression(Collection<Any2<MetricMatcher, TimeSeriesMetricExpression>> matchers, TagAggregationClause aggregation) {
        super("min", matchers, aggregation);
    }

    /* Calculate the sum of two numbers, preserving integral type if possible. */
    private static Number impl_(Number x, Number y) {
        return (x.doubleValue() > y.doubleValue() ? y : x);
    }

    @Override
    protected Optional<Number> initial_() { return Optional.empty(); }
    @Override
    protected Optional<Number> map_(MetricValue x) {
        final Optional<Number> hist_min = x.histogram().flatMap(Histogram::min).map(dbl -> dbl);
        if (hist_min.isPresent()) return hist_min;
        return x.value();
    }
    @Override
    protected MetricValue unmap_(Optional<Number> v) { return v.map(MetricValue::fromNumberValue).orElse(MetricValue.EMPTY); }
    @Override
    protected Optional<Number> reducer_(Optional<Number> x, Optional<Number> y) {
        if (!x.isPresent()) return y;
        if (!y.isPresent()) return x;
        return Optional.of(impl_(x.get(), y.get()));
    }
    @Override
    protected MetricValue scalar_fallback_() { return MetricValue.EMPTY; }

    @Override
    public int getPriority() {
        return BRACKETS;
    }
}
