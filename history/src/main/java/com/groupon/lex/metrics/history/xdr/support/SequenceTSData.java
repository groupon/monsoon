package com.groupon.lex.metrics.history.xdr.support;

import com.google.common.collect.Iterators;
import com.groupon.lex.metrics.history.TSData;
import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.lib.sequence.EqualRange;
import com.groupon.lex.metrics.lib.sequence.ForwardSequence;
import com.groupon.lex.metrics.lib.sequence.ObjectSequence;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.joda.time.DateTime;

/**
 * A TSData that is based around sequences.
 *
 * @author ariane
 */
public abstract class SequenceTSData implements TSData {
    public abstract ObjectSequence<TimeSeriesCollection> getSequence();

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

    @Override
    public boolean contains(Object o) {
        if (!(o instanceof TimeSeriesCollection))
            return false;
        ObjectSequence<TimeSeriesCollection> seq = getSequence();
        final EqualRange range = seq.equalRange(tsc -> tsc.compareTo((TimeSeriesCollection) o));
        if (range.isEmpty())
            return false;
        seq = seq.limit(range.getEnd()).skip(range.getBegin());
        return Iterators.contains(seq.iterator(), o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        if (c.isEmpty())
            return true;

        /* Only contains TimeSeriesCollections, so filter out the others. */
        final List<TimeSeriesCollection> tsc_list;
        try {
            tsc_list = c.stream()
                    .map(tsc -> (TimeSeriesCollection) tsc)
                    .collect(Collectors.toList());
        } catch (ClassCastException ex) {
            return false;  // Was not a TimeSeriesCollection.
        }

        final ObjectSequence<TimeSeriesCollection> needles = new ForwardSequence(0, tsc_list.size())
                .map(tsc_list::get, false, true, false)
                .sort();
        ObjectSequence<TimeSeriesCollection> haystack = getSequence();
        final EqualRange haystackRange = haystack.equalRange((tsc -> {
            if (tsc.compareTo(needles.first()) < 0)
                return -1;
            if (tsc.compareTo(needles.last()) > 0)
                return 1;
            return 0;
        }));
        haystack = haystack
                .limit(haystackRange.getEnd())
                .skip(haystackRange.getBegin());

        for (TimeSeriesCollection needle : needles) {
            haystack = haystack.skip(haystack.equalRange(tsc -> tsc.compareTo(needle)).getBegin());
            if (haystack.isEmpty() || !Objects.equals(haystack.first(), needle))
                return false;
        }
        return true;
    }

    @Override
    public Compression getAppendCompression() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setAppendCompression(Compression compression) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Compression getOptimizedCompression() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setOptimizedCompression(Compression compression) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
