package com.groupon.lex.metrics.timeseries;

import java.util.ArrayList;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.Optional;

/**
 *
 * @author ariane
 */
public class ImmutableTimeSeriesCollectionPair implements TimeSeriesCollectionPair {
    private final List<TimeSeriesCollection> history_;

    public ImmutableTimeSeriesCollectionPair(List<? extends TimeSeriesCollection> tsc) {
        history_ = unmodifiableList(tsc);
    }

    public static TimeSeriesCollectionPair copyList(List<? extends TimeSeriesCollection> tsc) {
        return new ImmutableTimeSeriesCollectionPair(new ArrayList<>(tsc));
    }

    @Override
    public TimeSeriesCollection getCurrentCollection() {
        if (history_.isEmpty())
            return new MutableTimeSeriesCollection();
        return history_.get(0);
    }

    @Override
    public Optional<TimeSeriesCollection> getPreviousCollection(int n) {
        if (n < 0) throw new IllegalArgumentException("cannot look into the future");
        if (n >= history_.size()) return Optional.empty();
        return Optional.of(history_.get(n));
    }

    @Override
    public int size() {
        return history_.size();
    }
}
