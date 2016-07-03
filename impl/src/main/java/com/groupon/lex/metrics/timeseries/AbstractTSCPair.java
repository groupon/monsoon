package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.lib.BufferedIterator;
import com.groupon.lex.metrics.lib.ForwardIterator;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.unmodifiableList;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

/**
 * A TimeSeriesCollectionPair that keeps track of historical data.
 */
public abstract class AbstractTSCPair implements TimeSeriesCollectionPair {
    private static final Logger LOG = Logger.getLogger(AbstractTSCPair.class.getName());
    private List<BackRefTimeSeriesCollection> previous_ = new ArrayList<>();

    public void initWithHistoricalData(CollectHistory history, ExpressionLookBack lookback) {
        if (!previous_.isEmpty()) {
            LOG.log(Level.WARNING, "skipping historical data initialization, as data is already present");
            return;
        }

        Stream<TimeSeriesCollection> filtered;
        try {
            filtered = lookback.filter(new ForwardIterator<>(BufferedIterator.stream(ForkJoinPool.commonPool(), history.streamReversed()).iterator()));
        } catch (UnsupportedOperationException ex) {
            LOG.log(Level.WARNING, "history reverse streaming not supported, fallback to duration hint");
            final DateTime end = history.getEnd();
            final DateTime begin = end.minus(lookback.hintDuration());
            filtered = history.stream(begin, end);
        }

        BackRefTimeSeriesCollection prev = new BackRefTimeSeriesCollection(new DateTime(0, DateTimeZone.UTC));
        for (TimeSeriesCollection tsc : filtered
                .sorted(Comparator.comparing(TimeSeriesCollection::getTimestamp))
                .distinct()
                .collect(Collectors.toList())) {
            prev.merge(tsc.getTimestamp(), tsc.getTSValues());
            previous_.add(0, prev.clone());
        }
        LOG.log(Level.INFO, "recovered {0} scrapes from history", previous_.size());
    }

    @Override
    public TimeSeriesCollection getPreviousCollection() {
        if (previous_.isEmpty()) return new MutableTimeSeriesCollection();
        return previous_.get(0);
    }

    @Override
    public Optional<TimeSeriesCollection> getPreviousCollection(int n) {
        if (n == 0) return Optional.of(getCurrentCollection());
        if (n == 1) return Optional.of(getPreviousCollection());
        if (n - 1 >= previous_.size()) return Optional.empty();
        return Optional.of(previous_.get(n - 1));
    }

    @Override
    public TimeSeriesCollectionPair getPreviousCollectionPair(int n) {
        if (n < 0) throw new IllegalArgumentException("cannot look into the future");
        if (n == 0) return this;
        if (n - 1 >= previous_.size()) return new ImmutableTimeSeriesCollectionPair(EMPTY_LIST);
        return ImmutableTimeSeriesCollectionPair.copyList(previous_.subList(n - 1, previous_.size()));
    }

    @Override
    public Optional<TimeSeriesCollection> getPreviousCollection(Duration duration) {
        final DateTime ts = getCurrentCollection().getTimestamp().minus(duration);
        if (!getCurrentCollection().getTimestamp().isAfter(ts)) return Optional.of(getCurrentCollection());

        for (TimeSeriesCollection p : previous_) {
            if (!p.getTimestamp().isAfter(ts))
                return Optional.of(p);
        }
        return Optional.empty();
    }

    @Override
    public TimeSeriesCollectionPair getPreviousCollectionPair(Duration duration) {
        final DateTime ts = getCurrentCollection().getTimestamp().minus(duration);
        if (!getCurrentCollection().getTimestamp().isAfter(ts)) return this;

        final ListIterator<BackRefTimeSeriesCollection> p = previous_.listIterator(0);
        while (p.hasNext()) {
            final int idx = p.nextIndex();
            final TimeSeriesCollection next = p.next();
            if (!next.getTimestamp().isAfter(ts))
                return ImmutableTimeSeriesCollectionPair.copyList(previous_.subList(idx, previous_.size()));
        }
        return new ImmutableTimeSeriesCollectionPair(EMPTY_LIST);
    }

    protected void update(MutableTimeSeriesCollection mtsc, ExpressionLookBack lookback) {
        if (!previous_.isEmpty()) {
            previous_.add(0, previous_.get(0).clone().merge(mtsc));  // Clone into position 0, so scrapes shared among classes remain unaffected.
        } else {
            previous_.add(new BackRefTimeSeriesCollection(mtsc.getTimestamp()).merge(mtsc));
        }
        apply_lookback_(lookback);
    }

    protected void update(DateTime ts, Collection<TimeSeriesValue> values, ExpressionLookBack lookback) {
        if (!previous_.isEmpty()) {
            previous_.add(0, previous_.get(0).clone().merge(ts, values));  // Clone into position 0, so scrapes shared among classes remain unaffected.
        } else {
            previous_.add(new BackRefTimeSeriesCollection(ts).merge(ts, values));
        }
        apply_lookback_(lookback);
    }

    protected void update(TimeSeriesCollection tsc, ExpressionLookBack lookback) {
        update(tsc.getTimestamp(), tsc.getTSValues(), lookback);
    }

    private void apply_lookback_(ExpressionLookBack lookback) {
        final List<BackRefTimeSeriesCollection> suggested_previous = lookback.filter(new ForwardIterator<>(unmodifiableList(previous_).iterator()))
                .distinct()
                .sorted(Comparator.comparing(TimeSeriesCollection::getTimestamp).reversed())
                .collect(Collectors.toList());

        if (suggested_previous.isEmpty())
            previous_.subList(1, previous_.size()).clear();
        else
            previous_ = suggested_previous;
    }

    @Override
    public String toString() {
        return previous_.stream()
                .map(TimeSeriesCollection::getTimestamp)
                .map(DateTime::toString)
                .collect(Collectors.joining(", ", "AbstractTSCPair[", "]"));
    }
}
