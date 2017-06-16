package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.MetricMatcher;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.timeseries.TagAggregationClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricAggregate;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class PercentileAggregateExpression extends TimeSeriesMetricAggregate<List<MetricValue>> {
    private static final Logger LOG = Logger.getLogger(PercentileAggregateExpression.class.getName());
    private final double percentile_;

    public PercentileAggregateExpression(double percentile, Collection<Any2<MetricMatcher, TimeSeriesMetricExpression>> matchers, TagAggregationClause aggregation, Optional<Duration> tDelta) {
        super("percentile_agg", matchers, aggregation, tDelta);
        percentile_ = percentile;
        if (percentile < 0.0 || percentile > 100.0)
            throw new IllegalArgumentException("invalid percentile range");
    }

    private MetricValue percentile_(Stream<MetricValue> values) {
        final List<Double> collected = values
                .map(MetricValue::value)
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .map(Number::doubleValue)
                .sorted()
                .collect(Collectors.toList());
        LOG.log(Level.FINEST, "collected {0}", collected);
        if (collected.isEmpty()) return MetricValue.EMPTY;

        final double dbl_index = (collected.size() - 1) * percentile_ / 100d;
        final int left = (int)Math.floor(dbl_index);
        final int right = (int)Math.ceil(dbl_index);
        final double left_fraction = dbl_index - left;
        final double right_fraction = 1d - left_fraction;
        LOG.log(Level.FINEST, "dbl_index={0}, left={1}, right={2}, left_fraction={3}, right_fraction={4}", new Object[]{dbl_index, left, right, left_fraction, right_fraction});

        final MetricValue result;
        if (right < collected.size()) {
            final double left_value = collected.get(left);
            final double right_value = collected.get(right);
            /*
             * Correct calculation of percentile is:
             *
             *   left + (right-left) * left_fraction
             * = left + right*left_fraction - left*left_fraction
             * = (1-left_fraction)*left + left_fraction*right
             * = right_fraction*left + left_fraction*right
             */
            result = MetricValue.fromDblValue(right_fraction * left_value + left_fraction * right_value);
        } else if (left < collected.size()) {
            result = MetricValue.fromDblValue(collected.get(left));
        } else {
            result = MetricValue.EMPTY;
        }
        LOG.log(Level.FINER, "{0} => P{1} = {2}", new Object[]{collected, percentile_, result});
        return result;
    }

    @Override
    protected List<MetricValue> initial_() { return EMPTY_LIST; }

    @Override
    protected List<MetricValue> map_(MetricValue v) {
        return singletonList(v);
    }

    @Override
    protected MetricValue unmap_(List<MetricValue> v) {
        return percentile_(v.stream());
    }

    @Override
    protected List<MetricValue> reducer_(List<MetricValue> x, List<MetricValue> y) {
        List<MetricValue> result = new ArrayList<>(x.size() + y.size());
        result.addAll(x);
        result.addAll(y);
        return result;
    }

    @Override
    protected String configStringArgs() {
        return percentile_ + ", " + super.configStringArgs();
    }

    @Override
    public int getPriority() {
        return BRACKETS;
    }
}
