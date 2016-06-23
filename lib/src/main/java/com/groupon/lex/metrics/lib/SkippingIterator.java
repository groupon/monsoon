/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.lib;

import java.util.Iterator;
import static java.util.Objects.requireNonNull;
import java.util.Spliterator;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class SkippingIterator<T> implements Iterator<T> {
    private final Iterator<T> underlying_;
    private final Function<? super T, ? extends DateTime> timestamp_fn_;
    private final Duration stepsize_;
    private T next_;
    private DateTime expected_ts_ = new DateTime(0L, DateTimeZone.UTC);

    public SkippingIterator(Iterator<T> underlying, Function<? super T, ? extends DateTime> timestamp_fn, Duration stepsize) {
        underlying_ = requireNonNull(underlying);
        timestamp_fn_ = requireNonNull(timestamp_fn);
        stepsize_ = requireNonNull(stepsize);
        next_ = null;
    }

    public static <T> Stream<T> adaptStream(Stream<T> stream, Function<? super T, ? extends DateTime> timestamp_fn, Duration stepsize) {
        if (stepsize.getMillis() <= 0L) return stream;

        final Iterator<T> iter = new SkippingIterator<>(stream.iterator(), timestamp_fn, stepsize);
        final Spliterator<T> spliter = Spliterators.spliteratorUnknownSize(iter, NONNULL | IMMUTABLE | ORDERED);
        return StreamSupport.stream(spliter, false);
    }

    @Override
    public boolean hasNext() {
        fixup_next_();
        return next_ != null;
    }

    @Override
    public T next() {
        fixup_next_();
        final T next = next_;
        next_ = null;
        return next;
    }

    private void fixup_next_() {
        while (next_ == null && underlying_.hasNext()) {
            final T next = underlying_.next();
            final DateTime next_ts = timestamp_fn_.apply(next);
            if (!next_ts.isBefore(expected_ts_)) {
                next_ = next;
                expected_ts_ = next_ts.plus(stepsize_);
            }
        }
    }
}
