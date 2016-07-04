package com.groupon.lex.metrics.history;

import com.groupon.lex.metrics.timeseries.AbstractTSCPair;
import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.timeseries.expression.ContextIdentifier;
import static java.util.Collections.EMPTY_MAP;
import java.util.Iterator;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 * @author ariane
 */
public class HistoryContext implements Context {
    private static class TSCPair extends AbstractTSCPair {
        private final TimeSeriesCollection current_;

        public TSCPair() {
            current_ = null;
        }

        private TSCPair(TSCPair origin, TimeSeriesCollection next, ExpressionLookBack lookback) {
            super(origin);
            if (origin.current_ != null) update(origin.current_, lookback);
            current_ = next;
        }

        @Override
        public TimeSeriesCollection getCurrentCollection() {
            if (current_ == null) throw new IllegalStateException("must advance first");
            return current_;
        }

        public TSCPair advance(TimeSeriesCollection next, ExpressionLookBack lookback) {
            return new TSCPair(this, next, lookback);
        }
    }

    private static class IteratorImpl implements Iterator<Context> {
        private final ExpressionLookBack lookback_;
        private final Iterator<TimeSeriesCollection> tsdata_iter_;
        private TSCPair previous_;

        public IteratorImpl(Iterator<TimeSeriesCollection> tsdata_iter, ExpressionLookBack lookback) {
            lookback_ = requireNonNull(lookback);
            tsdata_iter_ = requireNonNull(tsdata_iter);
            previous_ = new TSCPair();
        }

        @Override
        public boolean hasNext() {
            return tsdata_iter_.hasNext();
        }

        @Override
        public Context next() {
            final TSCPair next = previous_.advance(tsdata_iter_.next(), lookback_);
            previous_ = next;
            return new HistoryContext(next);
        }
    }

    private final TSCPair tsdata_;

    private HistoryContext(TSCPair tsdata) {
        tsdata_ = requireNonNull(tsdata);
    }

    @Override
    public Consumer<Alert> getAlertManager() {
        return (alert) -> {};
    }

    @Override
    public TimeSeriesCollectionPair getTSData() {
        return tsdata_;
    }

    @Override
    public Map<String, ContextIdentifier> getAllIdentifiers() {
        return EMPTY_MAP;
    }

    public static Iterator<Context> asIterator(Stream<TimeSeriesCollection> tsdata_stream, ExpressionLookBack lookback) {
        return asIterator(tsdata_stream.iterator(), lookback);
    }

    public static Iterator<Context> asIterator(Iterable<TimeSeriesCollection> tsdata_stream, ExpressionLookBack lookback) {
        return asIterator(tsdata_stream.iterator(), lookback);
    }

    public static Iterator<Context> asIterator(Iterator<TimeSeriesCollection> tsdata_stream, ExpressionLookBack lookback) {
        return new IteratorImpl(tsdata_stream, lookback);
    }

    public static Stream<Context> stream(Stream<TimeSeriesCollection> tsdata_stream, ExpressionLookBack lookback) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(asIterator(tsdata_stream, lookback), NONNULL | IMMUTABLE | ORDERED), false);
    }

    public static Stream<Context> stream(Iterable<TimeSeriesCollection> tsdata_stream, ExpressionLookBack lookback) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(asIterator(tsdata_stream, lookback), NONNULL | IMMUTABLE | ORDERED), false);
    }

    public static Stream<Context> stream(Iterator<TimeSeriesCollection> tsdata_stream, ExpressionLookBack lookback) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(asIterator(tsdata_stream, lookback), NONNULL | IMMUTABLE | ORDERED), false);
    }
}
