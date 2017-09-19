package com.groupon.lex.metrics.history;

import com.groupon.lex.metrics.GroupName;
import static com.groupon.lex.metrics.history.HistoryContext.LOOK_BACK;
import static com.groupon.lex.metrics.history.HistoryContext.LOOK_FORWARD;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricFilter;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.expression.Context;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public interface CollectHistory {
    public boolean add(TimeSeriesCollection tsv);

    public boolean addAll(Collection<? extends TimeSeriesCollection> c);

    public default boolean addAll(Iterator<? extends TimeSeriesCollection> i) {
        boolean changed = false;
        while (i.hasNext()) {
            if (add(i.next()))
                changed = true;
        }
        return changed;
    }

    public long getFileSize();

    /**
     * Get the highest timestamp covered by this TSData series.
     */
    public DateTime getEnd();

    /**
     * Stream the TSData contents in reverse chronological order.
     */
    public default Stream<TimeSeriesCollection> streamReversed() {
        throw new UnsupportedOperationException("history streamReversed");
    }

    /**
     * Stream the TSData contents.
     */
    public Stream<TimeSeriesCollection> stream();

    /**
     * Stream the TSData contents.
     */
    public default Stream<TimeSeriesCollection> stream(Duration stepsize) {
        return IntervalIterator.stream(stream(), stepsize, LOOK_BACK, LOOK_FORWARD);
    }

    /**
     * Stream all contents of the TSData, start at the 'begin' timestamp
     * (inclusive).
     */
    public default Stream<TimeSeriesCollection> stream(DateTime begin) {
        return stream()
                .filter((tsv) -> !begin.isAfter(tsv.getTimestamp()));
    }

    /**
     * Stream all occurances of the group since begin. If a scrape does not
     * contain the given group, it will be skipped.
     *
     * @param begin The timestamp from which to start streaming.
     * @param group The group for which to stream.
     * @return Returns a stream of Map.Entry, mapping the timestamp to the
     * selected group of time series values.
     */
    public default Stream<Map.Entry<DateTime, TimeSeriesValue>> streamGroup(DateTime begin, GroupName group) {
        return stream(begin)
                .map(tsc -> tsc.get(group).map(tsv -> SimpleMapEntry.create(tsc.getTimestamp(), tsv)))
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    /**
     * Stream all contents of the TSData, start at the 'begin' timestamp
     * (inclusive).
     */
    public default Stream<TimeSeriesCollection> stream(DateTime begin, Duration stepsize) {
        return IntervalIterator.stream(stream(begin.minus(LOOK_BACK)), stepsize, LOOK_BACK, LOOK_FORWARD)
                .filter(ts -> !ts.getTimestamp().isBefore(begin));
    }

    /**
     * Stream all contents of the TSData, between the 'begin' timestamp
     * (inclusive) and the 'end' timestamp (inclusive).
     */
    public default Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end) {
        return stream(begin)
                .filter((tsv) -> !end.isBefore(tsv.getTimestamp()));
    }

    /**
     * Stream all contents of the TSData, between the 'begin' timestamp
     * (inclusive) and the 'end' timestamp (inclusive).
     */
    public default Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end, Duration stepsize) {
        return IntervalIterator.stream(stream(begin.minus(LOOK_BACK), end.plus(LOOK_FORWARD)), stepsize, LOOK_BACK, LOOK_FORWARD)
                .filter(ts -> !ts.getTimestamp().isBefore(begin) && !ts.getTimestamp().isAfter(end));
    }

    /**
     * Return a History Context for evaluating expressions.
     */
    public default Stream<Context> getContext(Duration stepsize, ExpressionLookBack lookback, TimeSeriesMetricFilter filter) {
        return HistoryContext.stream(stream(stepsize), lookback);
    }

    /**
     * Return a History Context for evaluating expressions, starting at the
     * 'begin' timestamp (inclusive).
     */
    public default Stream<Context> getContext(DateTime begin, Duration stepsize, ExpressionLookBack lookback, TimeSeriesMetricFilter filter) {
        return HistoryContext.stream(stream(begin.minus(lookback.hintDuration()), stepsize), lookback)
                .filter(ctx -> !ctx.getTSData().getCurrentCollection().getTimestamp().isBefore(begin));
    }

    /**
     * Return a History Context for evaluating expressions, between the 'begin'
     * timestamp (inclusive) and the 'end' timestamp (inclusive).
     */
    public default Stream<Context> getContext(DateTime begin, DateTime end, Duration stepsize, ExpressionLookBack lookback, TimeSeriesMetricFilter filter) {
        return HistoryContext.stream(stream(begin.minus(lookback.hintDuration()), end, stepsize), lookback)
                .filter(ctx -> !ctx.getTSData().getCurrentCollection().getTimestamp().isBefore(begin));
    }

    /**
     * Evaluate expression over time.
     */
    public default Stream<Collection<NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> expression, Duration stepsize) {
        final TimeSeriesMetricFilter filter = expression.values().stream()
                .map(TimeSeriesMetricExpression::getNameFilter)
                .reduce(TimeSeriesMetricFilter::with)
                .orElse(TimeSeriesMetricFilter.ALL_GROUPS);
        final ExpressionLookBack lookback = ExpressionLookBack.EMPTY
                .andThen(expression.values().stream().map(TimeSeriesMetricExpression::getLookBack));
        return new ApplyExpressions(expression).apply(getContext(stepsize, lookback, filter));
    }

    /**
     * Evaluate expression over time, starting at the 'begin' timestamp
     * (inclusive).
     */
    public default Stream<Collection<NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> expression, DateTime begin, Duration stepsize) {
        final TimeSeriesMetricFilter filter = expression.values().stream()
                .map(TimeSeriesMetricExpression::getNameFilter)
                .reduce(TimeSeriesMetricFilter::with)
                .orElse(TimeSeriesMetricFilter.ALL_GROUPS);
        final ExpressionLookBack lookback = ExpressionLookBack.EMPTY
                .andThen(expression.values().stream().map(TimeSeriesMetricExpression::getLookBack));
        return new ApplyExpressions(expression).apply(getContext(begin, stepsize, lookback, filter));
    }

    /**
     * Evaluate expression over time, between the 'begin' timestamp (inclusive)
     * and the 'end' timestamp (inclusive).
     */
    public default Stream<Collection<NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> expression, DateTime begin, DateTime end, Duration stepsize) {
        final TimeSeriesMetricFilter filter = expression.values().stream()
                .map(TimeSeriesMetricExpression::getNameFilter)
                .reduce(TimeSeriesMetricFilter::with)
                .orElse(TimeSeriesMetricFilter.ALL_GROUPS);
        final ExpressionLookBack lookback = ExpressionLookBack.EMPTY
                .andThen(expression.values().stream().map(TimeSeriesMetricExpression::getLookBack));
        return new ApplyExpressions(expression).apply(getContext(begin, end, stepsize, lookback, filter));
    }

    @RequiredArgsConstructor
    public static class ApplyExpressions implements Function<Stream<Context>, Stream<Collection<NamedEvaluation>>> {
        private final Map<String, ? extends TimeSeriesMetricExpression> expression;

        @Override
        public Stream<Collection<NamedEvaluation>> apply(Stream<Context> ctxStream) {
            return ctxStream.map(ctx -> {
                return expression.entrySet().stream()
                        .map(eval -> {
                            return new NamedEvaluation(eval.getKey(), ctx.getTSData().getCurrentCollection().getTimestamp(), eval.getValue().apply(ctx));
                        })
                        .collect(Collectors.toList());
            });
        }
    }

    @Value
    public static class NamedEvaluation {
        @NonNull
        private final String name;
        @NonNull
        private final DateTime datetime;
        @NonNull
        private final TimeSeriesMetricDeltaSet tS;
    }
}
