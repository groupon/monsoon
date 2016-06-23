package com.groupon.lex.metrics.history;

import com.groupon.lex.metrics.lib.BufferedIterator;
import com.groupon.lex.metrics.lib.SkippingIterator;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.expression.Context;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
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
    public long getFileSize();
    /** Get the highest timestamp covered by this TSData series. */
    public DateTime getEnd();

    /** Stream the TSData contents in reverse chronological order. */
    public default Stream<TimeSeriesCollection> streamReversed() {
        throw new UnsupportedOperationException("history streamReversed");
    }

    /** Stream the TSData contents. */
    public Stream<TimeSeriesCollection> stream();
    /** Stream the TSData contents. */
    public default Stream<TimeSeriesCollection> stream(Duration stepsize) {
        return SkippingIterator.adaptStream(stream(), TimeSeriesCollection::getTimestamp, stepsize);
    }

    /** Stream all contents of the TSData, start at the 'begin' timestamp (inclusive). */
    public default Stream<TimeSeriesCollection> stream(DateTime begin) {
        return stream()
                .filter((tsv) -> !begin.isAfter(tsv.getTimestamp()));
    }
    /** Stream all contents of the TSData, start at the 'begin' timestamp (inclusive). */
    public default Stream<TimeSeriesCollection> stream(DateTime begin, Duration stepsize) {
        return SkippingIterator.adaptStream(stream(begin), TimeSeriesCollection::getTimestamp, stepsize);
    }

    /** Stream all contents of the TSData, between the 'begin' timestamp (inclusive) and the 'end' timestamp (inclusive). */
    public default Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end) {
        return stream(begin)
                .filter((tsv) -> !end.isBefore(tsv.getTimestamp()));
    }
    /** Stream all contents of the TSData, between the 'begin' timestamp (inclusive) and the 'end' timestamp (inclusive). */
    public default Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end, Duration stepsize) {
        return SkippingIterator.adaptStream(stream(begin, end), TimeSeriesCollection::getTimestamp, stepsize);
    }

    /** Return a History Context for evaluating expressions. */
    public default Stream<Context> getContext(Duration stepsize, ExpressionLookBack lookback) {
        return BufferedIterator.stream(ForkJoinPool.commonPool(), HistoryContext.stream(stream(stepsize), lookback));
    }

    /** Return a History Context for evaluating expressions, starting at the 'begin' timestamp (inclusive). */
    public default Stream<Context> getContext(DateTime begin, Duration stepsize, ExpressionLookBack lookback) {
        return BufferedIterator.stream(ForkJoinPool.commonPool(), HistoryContext.stream(stream(begin.minus(lookback.hintDuration()), stepsize), lookback)
                .filter(ctx -> !ctx.getTSData().getCurrentCollection().getTimestamp().isBefore(begin)));
    }

    /** Return a History Context for evaluating expressions, between the 'begin' timestamp (inclusive) and the 'end' timestamp (inclusive). */
    public default Stream<Context> getContext(DateTime begin, DateTime end, Duration stepsize, ExpressionLookBack lookback) {
        return BufferedIterator.stream(ForkJoinPool.commonPool(), HistoryContext.stream(stream(begin.minus(lookback.hintDuration()), end, stepsize), lookback)
                .filter(ctx -> !ctx.getTSData().getCurrentCollection().getTimestamp().isBefore(begin)));
    }

    /** Evaluate expression over time. */
    public default Stream<Collection<NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> expression, Duration stepsize) {
        final ExpressionLookBack lookback = ExpressionLookBack.EMPTY.andThen(expression.values().stream().map(TimeSeriesMetricExpression::getLookBack));
        return getContext(stepsize, lookback)
                .map(ctx -> {
                    return expression.entrySet().stream()
                            .map(eval -> {
                                return new NamedEvaluation(eval.getKey(), ctx.getTSData().getCurrentCollection().getTimestamp(), eval.getValue().apply(ctx));
                            })
                            .collect(Collectors.toList());
                });
    }

    /** Evaluate expression over time, starting at the 'begin' timestamp (inclusive). */
    public default Stream<Collection<NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> expression, DateTime begin, Duration stepsize) {
        final ExpressionLookBack lookback = ExpressionLookBack.EMPTY.andThen(expression.values().stream().map(TimeSeriesMetricExpression::getLookBack));
        return getContext(begin, stepsize, lookback)
                .map(ctx -> {
                    return expression.entrySet().stream()
                            .map(eval -> {
                                return new NamedEvaluation(eval.getKey(), ctx.getTSData().getCurrentCollection().getTimestamp(), eval.getValue().apply(ctx));
                            })
                            .collect(Collectors.toList());
                });
    }

    /** Evaluate expression over time, between the 'begin' timestamp (inclusive) and the 'end' timestamp (inclusive). */
    public default Stream<Collection<NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> expression, DateTime begin, DateTime end, Duration stepsize) {
        final ExpressionLookBack lookback = ExpressionLookBack.EMPTY.andThen(expression.values().stream().map(TimeSeriesMetricExpression::getLookBack));
        return getContext(begin, end, stepsize, lookback)
                .map(ctx -> {
                    return expression.entrySet().stream()
                            .map(eval -> {
                                return new NamedEvaluation(eval.getKey(), ctx.getTSData().getCurrentCollection().getTimestamp(), eval.getValue().apply(ctx));
                            })
                            .collect(Collectors.toList());
                });
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
