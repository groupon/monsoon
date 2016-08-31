package com.groupon.lex.metrics;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.Value;

/**
 * A histogram with buckets indexed by a value.
 *
 * A histogram contains zero or more buckets, each with a count of the number
 * of events in that bucket.
 * @author ariane
 */
public class Histogram implements Serializable, Comparable<Histogram> {
    /** Buckets, sorted by range. */
    private final List<Bucket> buckets_;

    /**
     * Create a histogram from a set of ranges with associated event counters.
     * @throws IllegalArgumentException If the items contain mixed signs.
     */
    public Histogram(RangeWithCount... items) {
        this(Arrays.stream(items));
    }

    /**
     * Create a histogram from a set of ranges with associated event counters.
     * @throws IllegalArgumentException If the items contain mixed signs.
     */
    public Histogram(Stream<RangeWithCount> items) {
        final List<RangeWithCount> iter = cleanup_(items.map(RangeWithCount::clone).collect(Collectors.toList()));
        if (iter.isEmpty()) {
            buckets_ = EMPTY_LIST;
            return;
        }

        if (iter.stream()
                .map(RangeWithCount::getCount)
                .map(Math::signum)
                .distinct()
                .count() > 1) {
            throw new IllegalArgumentException("mixed sign");
        }

        final List<Bucket> buckets = new ArrayList<>(iter.size());
        double running_total = 0;
        for (RangeWithCount rwc : iter) {
            running_total += rwc.getCount();
            buckets.add(new Bucket(rwc.getRange(), rwc.getCount(), running_total));
        }
        buckets_ = unmodifiableList(buckets);
    }

    /**
     * Returns a map of range -&gt; event count.
     * The elements of the stream are a mutable copy of the internal data.
     */
    public Stream<RangeWithCount> stream() {
        return buckets_.stream()
                .map(bucket -> new RangeWithCount(bucket.getRange(), bucket.getEvents()));
    }

    /** Return the event count of this histogram. */
    public double getEventCount() {
        return (buckets_.isEmpty() ? 0 : buckets_.get(buckets_.size() - 1).getRunningEventsCount());
    }

    /** Test if the histogram is empty. */
    public boolean isEmpty() { return buckets_.isEmpty(); }

    /** Return the minimum value in the histogram. */
    public Optional<Double> min() {
        if (isEmpty()) return Optional.empty();
        return Optional.of(buckets_.get(0).getRange().getFloor());
    }

    /** Return the minimum value in the histogram. */
    public Optional<Double> max() {
        if (isEmpty()) return Optional.empty();
        return Optional.of(buckets_.get(buckets_.size() - 1).getRange().getCeil());
    }

    /** Return the median of the histogram. */
    public Optional<Double> median() {
        if (isEmpty()) return Optional.empty();
        return Optional.of(percentile(50));
    }

    /** Return the average of the histogram. */
    public Optional<Double> avg() {
        if (isEmpty()) return Optional.empty();
        return Optional.of(sum() / getEventCount());
    }

    /** Return the sum of the histogram. */
    public double sum() {
        return buckets_.stream()
                .mapToDouble(b -> b.getRange().getMidPoint() * b.getEvents())
                .sum();
    }

    /** Get the value at a given position. */
    public double get(double index) {
        ListIterator<Bucket> b = buckets_.listIterator(0);
        ListIterator<Bucket> e = buckets_.listIterator(buckets_.size());

        while (b.nextIndex() < e.previousIndex()) {
            final ListIterator<Bucket> mid = buckets_.listIterator(b.nextIndex() / 2 + e.nextIndex() / 2);
            final Bucket mid_bucket = mid.next();
            mid.previous();  // Undo position change made by mid.next().

            if (mid_bucket.getRunningEventsCount() == index && mid.nextIndex() >= e.previousIndex()) {
                return mid_bucket.getRange().getCeil();
            } else if (mid_bucket.getRunningEventsCount() <= index) {
                b = mid;
                b.next();
            } else if (mid_bucket.getRunningEventsCount() - mid_bucket.getEvents() > index) {
                e = mid;
            } else {
                b = mid;
                break;
            }
        }

        final Bucket bucket = b.next();
        b.previous();  // Undo position change made by b.next().
        final double low = bucket.getRunningEventsCount() - bucket.getEvents();
        final double off = index - low;
        final double left_fraction = off / bucket.getEvents();
        final double right_fraction = 1 - left_fraction;
        return bucket.getRange().getCeil() * left_fraction + bucket.getRange().getFloor() * right_fraction;
    }

    /** Get the value at the given percentile. */
    public double percentile(double percentile) {
        return get(percentile * getEventCount() / 100);
    }

    /** Create a new histogram, after applying the function on each of the event counters. */
    public Histogram modifyEventCounters(BiFunction<Range, Double, Double> fn) {
        return new Histogram(stream()
                .map(entry -> {
                    entry.setCount(fn.apply(entry.getRange(), entry.getCount()));
                    return entry;
                }));
    }

    /** Add two histograms together. */
    public static Histogram add(Histogram x, Histogram y) {
        return new Histogram(Stream.concat(x.stream(), y.stream()));
    }

    /** Negates the counters on the histogram. */
    public static Histogram negate(Histogram x) {
        return x.modifyEventCounters((r, d) -> -d);
    }

    /**
     * Subtracts two histograms.
     * @throws IllegalArgumentException If the result contains mixed signs.
     */
    public static Histogram subtract(Histogram x, Histogram y) {
        return new Histogram(Stream.concat(
                x.stream(),
                y.stream().map(rwc -> {
                    rwc.setCount(-rwc.getCount());
                    return rwc;
                })));
    }

    /** Add scalar to histogram. */
    public static Histogram add(Histogram x, double y) {
        return x.modifyEventCounters((r, d) -> d + r.getWidth() * y);
    }

    /** Subtract scalar to histogram. */
    public static Histogram subtract(Histogram x, double y) {
        return x.modifyEventCounters((r, d) -> d - r.getWidth() * y);
    }

    /** Multiply histogram by scalar. */
    public static Histogram multiply(Histogram x, double y) {
        return x.modifyEventCounters((r, d) -> d * y);
    }

    /** Divide histogram by scalar. */
    public static Histogram divide(Histogram x, double y) {
        return x.modifyEventCounters((r, d) -> d / y);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + Objects.hashCode(this.buckets_);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Histogram other = (Histogram) obj;
        if (!Objects.equals(this.buckets_, other.buckets_)) {
            return false;
        }
        return true;
    }

    /** Compare two histograms. */
    @Override
    public int compareTo(Histogram o) {
        int cmp = 0;
        final Iterator<Bucket> iter = buckets_.iterator(), o_iter = o.buckets_.iterator();
        while (cmp == 0 && iter.hasNext() && o_iter.hasNext()) {
            final Bucket next = iter.next(), o_next = o_iter.next();
            cmp = Double.compare(next.getRange().getFloor(), o_next.getRange().getFloor());
            if (cmp == 0)
                cmp = Double.compare(next.getRange().getCeil(), o_next.getRange().getCeil());
            if (cmp == 0)
                cmp = Double.compare(next.getEvents(), o_next.getEvents());
        }

        if (cmp == 0)
            cmp = (iter.hasNext() ? 1 : (o_iter.hasNext() ? -1 : 0));
        return cmp;
    }

    @Override
    public String toString() {
        return buckets_.stream()
                .map(bucket -> bucket.getRange().getFloor() + ".." + bucket.getRange().getCeil() + "=" + bucket.getEvents())
                .collect(Collectors.joining(", ", "[ ", " ]"));
    }

    @Value
    public static class Range implements Serializable {
        private final double floor, ceil;

        /**
         * Constructor.
         * @throws IllegalArgumentException If the ceil is less than the floor.
         */
        public Range(double floor, double ceil) {
            if (floor > ceil) throw new IllegalArgumentException("negative range");
            this.floor = floor;
            this.ceil = ceil;
        }

        /** Returns the width of this range. */
        public double getWidth() { return getCeil() - getFloor(); }

        /** Returns the mid-point of this range. */
        public double getMidPoint() { return getFloor() / 2 + getCeil() / 2; }
    }

    @Value
    private static class Bucket implements Serializable {
        @NonNull
        private final Range range;
        private final double events;
        private final double runningEventsCount;
    }

    @Data
    @AllArgsConstructor
    public static class RangeWithCount implements Serializable, Cloneable {
        private Range range;
        private double count;

        public RangeWithCount(double floor, double ceil, double count) {
            this(new Range(floor, ceil), count);
        }

        @Override
        public RangeWithCount clone() {
            return new RangeWithCount(range, count);
        }
    }

    /**
     * Clean up an arbitrary collection of ranges.
     *
     * Ranges are split at their intersection point.
     * Ranges with a count of zero are omitted.
     * @param imed An arbitrary collection of ranges.  The operations on this list will be destructive.
     * @return An ordered list of ranges, none of which intersect eachother.
     */
    private static List<RangeWithCount> cleanup_(List<RangeWithCount> imed) {
        final Comparator<RangeWithCount> cmp = Comparator
                .comparing((RangeWithCount range_count) -> range_count.getRange().getFloor())
                .thenComparing(Comparator.comparing((RangeWithCount range_count) -> range_count.getRange().getCeil()));

        final List<RangeWithCount> result = new ArrayList<>(imed.size());
        sort(imed, cmp);

        while (imed.size() >= 2) {
            final RangeWithCount head = imed.remove(0);
            final RangeWithCount succ = imed.get(0);

            // Merge adjecent ranges.
            if (head.getRange().equals(succ.getRange())) {
                succ.setCount(succ.getCount() + head.getCount());
                continue;
            }

            // Move elements from extending range.
            if (head.getRange().getFloor() == succ.getRange().getFloor()) {
                final double mid = head.getRange().getCeil();
                final double ceil = succ.getRange().getCeil();
                final double succ_range = succ.getRange().getWidth();

                final double succ_left_fraction = (mid - succ.getRange().getFloor()) / succ_range;
                final double succ_right_fraction = 1 - succ_left_fraction;

                head.setCount(head.getCount() + succ_left_fraction * succ.getCount());
                succ.setCount(succ_right_fraction * succ.getCount());
                succ.setRange(new Range(mid, ceil));
                imed.add(0, head);
                sort(imed, cmp);
                continue;
            }

            // Emit disjunt head range.
            if (head.getRange().getCeil() <= succ.getRange().getFloor()) {
                if (Math.signum(head.getCount()) != 0)
                    result.add(head);
                continue;
            }

            // head.floor < succ.floor < head.ceil
            assert(head.getRange().getFloor() < succ.getRange().getFloor());
            assert(succ.getRange().getFloor() < head.getRange().getCeil());
            // Head is intersected by succ, split it in two, at the succ.floor boundary.
            final double floor = head.getRange().getFloor();
            final double ceil = succ.getRange().getFloor();
            final double head_range = head.getRange().getWidth();
            final double head_left_fraction = (ceil - floor) / head_range;
            final double head_right_fraction = 1 - head_left_fraction;
            imed.add(0, head);
            imed.add(0, new RangeWithCount(new Range(floor, ceil), head_left_fraction * head.getCount()));
            head.setRange(new Range(ceil, head.getRange().getCeil()));
            head.setCount(head_right_fraction * head.getCount());
            sort(imed, cmp);
        }

        imed.stream()
                .filter(rwc -> Math.signum(rwc.getCount()) !=  0)
                .forEach(result::add);

        // Merge adjecent entries, if they have the same distribution.
        for (int i = 0; i < result.size() - 1; ) {
            final RangeWithCount pred = result.get(i);
            final RangeWithCount succ = result.get(i + 1);
            final double pred_range = pred.getRange().getWidth();
            final double succ_range = succ.getRange().getWidth();

            if (pred.getRange().getCeil() == succ.getRange().getFloor() &&
                    pred.getCount() * succ_range == succ.getCount() * pred_range) {
                result.remove(i);
                succ.setRange(new Range(pred.getRange().getFloor(), succ.getRange().getCeil()));
                succ.setCount(succ.getCount() + pred.getCount());
            } else {
                ++i;
            }
        }

        return result;
    }
}
