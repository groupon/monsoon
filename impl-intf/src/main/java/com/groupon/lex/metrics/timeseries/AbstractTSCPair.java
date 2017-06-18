package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.lib.ForwardIterator;
import java.util.ArrayList;
import static java.util.Collections.unmodifiableList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.joda.time.DateTime;

/**
 * A TimeSeriesCollectionPair that keeps track of historical data.
 */
public abstract class AbstractTSCPair implements TimeSeriesCollectionPair {
    private static final Logger LOG = Logger.getLogger(AbstractTSCPair.class.getName());
    private List<TimeSeriesCollection> previous_;

    protected AbstractTSCPair() {
        previous_ = new ArrayList<>();
    }

    protected AbstractTSCPair(AbstractTSCPair original) {
        previous_ = new ArrayList<>(original.previous_);
    }

    private void validatePrevious() {
        try {
            DateTime ts = getCurrentCollection().getTimestamp();

            for (TimeSeriesCollection p : previous_) {
                if (!p.getTimestamp().isBefore(ts))
                    throw new IllegalArgumentException("previous timestamps must be before current and ordered in reverse chronological order");
                ts = p.getTimestamp();
            }
        } catch (IllegalArgumentException ex) {
            LOG.log(Level.SEVERE, "programmer error", ex);
            throw ex;
        }
    }

    @Override
    public abstract TimeSeriesCollection getCurrentCollection();

    @Override
    public Optional<TimeSeriesCollection> getPreviousCollection(int n) {
        if (n < 0)
            throw new IllegalArgumentException("cannot look into the future");
        if (n == 0) return Optional.of(getCurrentCollection());
        if (n - 1 >= previous_.size()) return Optional.empty();
        return Optional.of(previous_.get(n - 1));
    }

    protected final void update(TimeSeriesCollection tsc, ExpressionLookBack lookback, Runnable doBeforeValidation) {
        previous_.add(0, tsc);
        apply_lookback_(lookback);
        doBeforeValidation.run();

        validatePrevious();
    }

    private void apply_lookback_(ExpressionLookBack lookback) {
        final List<TimeSeriesCollection> suggested_previous = lookback.filter(new ForwardIterator<>(unmodifiableList(previous_).iterator()))
                .distinct()
                .sorted(Comparator.comparing(TimeSeriesCollection::getTimestamp).reversed())
                .collect(Collectors.toList());

        if (suggested_previous.isEmpty()) // Always keep at least 1 element.
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
