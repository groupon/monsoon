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
        private TimeSeriesCollection current_;
        private final Iterator<TimeSeriesCollection> iter_;

        public TSCPair(Iterator<TimeSeriesCollection> iter) {
            iter_ = requireNonNull(iter);
            current_ = null;
        }

        @Override
        public TimeSeriesCollection getCurrentCollection() {
            if (current_ == null) throw new IllegalStateException("must advance first");
            return current_;
        }

        public boolean hasNext() { return iter_.hasNext(); }

        public void advance(ExpressionLookBack lookback) {
            if (current_ != null) update(current_, lookback);
            current_ = iter_.next();
        }
    }

    private class IteratorImpl implements Iterator<Context> {
        private final ExpressionLookBack lookback_;

        public IteratorImpl(ExpressionLookBack lookback) {
            lookback_ = requireNonNull(lookback);
        }

        @Override
        public boolean hasNext() {
            return tsdata_.hasNext();
        }

        @Override
        public Context next() {
            advance(lookback_);
            return HistoryContext.this;
        }
    }

    private final TSCPair tsdata_;

    private HistoryContext(Iterator<TimeSeriesCollection> tsdata_iter) {
        tsdata_ = new TSCPair(tsdata_iter);
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

    public void advance(ExpressionLookBack lookback) {
        tsdata_.advance(lookback);
    }

    private Iterator<Context> as_iterator_(ExpressionLookBack lookback) {
        return new IteratorImpl(lookback);
    }

    public static Iterator<Context> asIterator(Stream<TimeSeriesCollection> tsdata_stream, ExpressionLookBack lookback) {
        return asIterator(tsdata_stream.iterator(), lookback);
    }

    public static Iterator<Context> asIterator(Iterable<TimeSeriesCollection> tsdata_stream, ExpressionLookBack lookback) {
        return asIterator(tsdata_stream.iterator(), lookback);
    }

    public static Iterator<Context> asIterator(Iterator<TimeSeriesCollection> tsdata_stream, ExpressionLookBack lookback) {
        return new HistoryContext(tsdata_stream).as_iterator_(lookback);
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
