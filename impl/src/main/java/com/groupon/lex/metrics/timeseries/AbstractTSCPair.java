package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.lib.ForwardIterator;
import java.util.ArrayList;
import static java.util.Collections.unmodifiableList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.joda.time.DateTime;

/**
 * A TimeSeriesCollectionPair that keeps track of historical data.
 */
public abstract class AbstractTSCPair implements TimeSeriesCollectionPair {
    private static final Logger LOG = Logger.getLogger(AbstractTSCPair.class.getName());
    private List<BackRefTimeSeriesCollection> previous_;

    protected AbstractTSCPair() {
        previous_ = new ArrayList<>();
    }

    protected AbstractTSCPair(AbstractTSCPair original) {
        previous_ = new ArrayList<>(original.previous_);
    }

    @Override
    public abstract TimeSeriesCollection getCurrentCollection();

    public void initWithHistoricalData(CollectHistory history, ExpressionLookBack lookback) {
        if (!previous_.isEmpty()) {
            LOG.log(Level.WARNING, "skipping historical data initialization, as data is already present");
            return;
        }

        Stream<TimeSeriesCollection> filtered;
        try {
            filtered = lookback.filter(new ForwardIterator<>(history.streamReversed().iterator()));
        } catch (UnsupportedOperationException ex) {
            LOG.log(Level.WARNING, "history reverse streaming not supported, fallback to duration hint");
            final DateTime end = history.getEnd();
            final DateTime begin = end.minus(lookback.hintDuration());
            filtered = history.stream(begin, end);
        }

        for (TimeSeriesCollection tsc : filtered
                .sorted(Comparator.comparing(TimeSeriesCollection::getTimestamp))
                .distinct()
                .collect(Collectors.toList())) {
            previous_.add(0, new BackRefTimeSeriesCollection(tsc.getTimestamp(), tsc.getTSValues()));
        }
        LOG.log(Level.INFO, "recovered {0} scrapes from history", previous_.size());
    }

    @Override
    public Optional<TimeSeriesCollection> getPreviousCollection(int n) {
        if (n < 0) throw new IllegalArgumentException("cannot look into the future");
        if (n == 0) return Optional.of(getCurrentCollection());
        if (n - 1 >= previous_.size()) return Optional.empty();
        return Optional.of(previous_.get(n - 1));
    }

    protected void update(TimeSeriesCollection tsc, ExpressionLookBack lookback) {
        previous_.add(0, new BackRefTimeSeriesCollection(tsc));
        apply_lookback_(lookback);
    }

    private void apply_lookback_(ExpressionLookBack lookback) {
        final List<BackRefTimeSeriesCollection> suggested_previous = lookback.filter(new ForwardIterator<>(unmodifiableList(previous_).iterator()))
                .distinct()
                .sorted(Comparator.comparing(TimeSeriesCollection::getTimestamp).reversed())
                .collect(Collectors.toList());

        if (suggested_previous.isEmpty())  // Always keep at least 1 element.
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

    @Override
    public int size() {
        return previous_.size() + 1;
    }
}
