package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.history.TSData;
import com.groupon.lex.metrics.lib.sequence.EqualRange;
import com.groupon.lex.metrics.lib.sequence.ObjectSequence;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.stream.Stream;
import org.joda.time.DateTime;

/**
 * A TSData that is based around sequences.
 *
 * @author ariane
 */
public abstract class SequenceTSData implements TSData {
    protected abstract ObjectSequence<TimeSeriesCollection> getSequence();

    @Override
    public Iterator<TimeSeriesCollection> iterator() {
        return getSequence().iterator();
    }

    @Override
    public Spliterator<TimeSeriesCollection> spliterator() {
        return getSequence().spliterator();
    }

    @Override
    public Stream<TimeSeriesCollection> stream() {
        return getSequence().stream();
    }

    @Override
    public Stream<TimeSeriesCollection> parallelStream() {
        return getSequence().parallelStream();
    }

    @Override
    public Stream<TimeSeriesCollection> streamReversed() {
        return getSequence()
                .reverse()
                .stream();
    }

    @Override
    public boolean isEmpty() {
        return getSequence().isEmpty();
    }

    @Override
    public int size() {
        return getSequence().size();
    }

    @Override
    public Object[] toArray() {
        return getSequence().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return getSequence().toArray(a);
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin) {
        if (getEnd().isBefore(begin))
            return ObjectSequence.<TimeSeriesCollection>empty().stream();

        ObjectSequence<TimeSeriesCollection> seq = getSequence();
        if (getBegin().isBefore(begin))
            seq = seq.skip(seq.equalRange((tsc) -> tsc.getTimestamp().compareTo(begin)).getBegin());
        return seq.stream();
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end) {
        if (getEnd().isBefore(begin) || getBegin().isAfter(end))
            return ObjectSequence.<TimeSeriesCollection>empty().stream();

        ObjectSequence<TimeSeriesCollection> seq = getSequence();
        EqualRange range = seq.equalRange((tsc) -> {
            if (tsc.getTimestamp().isBefore(begin))
                return -1;
            if (tsc.getTimestamp().isAfter(end))
                return 1;
            return 0;
        });
        return seq
                .limit(range.getEnd())
                .skip(range.getBegin())
                .stream();
    }
}
