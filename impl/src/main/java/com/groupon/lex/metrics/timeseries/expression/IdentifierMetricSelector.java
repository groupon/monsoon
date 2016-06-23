package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.ConfigSupport;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import java.util.Collection;
import java.util.Collections;

/**
 *
 * @author ariane
 */
public class IdentifierMetricSelector implements TimeSeriesMetricExpression {
    private final String identifier_;

    public IdentifierMetricSelector(String identifier) {
        identifier_ = identifier;
    }

    @Override
    public Collection<TimeSeriesMetricExpression> getChildren() { return Collections.EMPTY_LIST; }

    @Override
    public TimeSeriesMetricDeltaSet apply(Context ctx) {
        return ctx.getMetricFromIdentifier(identifier_)
                .orElseGet(TimeSeriesMetricDeltaSet::new);
    }

    @Override
    public int getPriority() {
        return BRACKETS;
    }

    @Override
    public StringBuilder configString() {
        return ConfigSupport.maybeQuoteIdentifier(identifier_);
    }

    @Override
    public String toString() { return configString().toString(); }
}
