/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved. 
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met: 
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer. 
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. 
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import java.util.Collection;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.EMPTY_SET;
import static java.util.Collections.unmodifiableSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public class TimeSeriesMetricDeltaSet {
    private final Any2<MetricValue, Map<Tags, MetricValue>> values_;

    /**
     * Create an untagged metric.
     * @param td The scalar value.
     */
    public TimeSeriesMetricDeltaSet(MetricValue td) {
        values_ = Any2.left(td);
    }

    /**
     * Create an empty MetricDelta set for tagged metrics.
     */
    public TimeSeriesMetricDeltaSet() {
        this(EMPTY_MAP);
    }

    /**
     * Create a MetricDelta set for tagged metrics and initialize it with the given collection.
     * @param td Initial set of tagged metric deltas.
     */
    public TimeSeriesMetricDeltaSet(Map<Tags, MetricValue> td) {
        this(td.entrySet().stream());
    }

    /**
     * Create a MetricDelta set for tagged metrics and initialize it with the given collection.
     * @param td Initial set of tagged metric deltas.
     */
    public TimeSeriesMetricDeltaSet(Stream<Entry<Tags, MetricValue>> td) {
        values_ = Any2.right(td.collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                (u,v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); },
                HashMap<Tags, MetricValue>::new)));
    }

    public boolean isEmpty() {
        return values_.mapCombine((x) -> false, (x) -> x.isEmpty());
    }
    public int size() {
        return values_.mapCombine((x) -> 1, (x) -> x.size());
    }

    public boolean isScalar() {
        return values_.mapCombine((x) -> true, (x) -> false);
    }
    public boolean isVector() {
        return values_.mapCombine((x) -> false, (x) -> true);
    }

    public Optional<MetricValue> asScalar() { return values_.getLeft(); }
    public Optional<Map<Tags, MetricValue>> asVector() { return values_.getRight(); }
    public Set<Tags> getTags() { return values_.mapCombine(scalar -> EMPTY_SET, map -> unmodifiableSet(map.keySet())); }

    public Stream<MetricValue> streamValues() {
        return values_.mapCombine(Stream::of, map -> map.values().stream());
    }

    /**
     * Retrieve the values in this set, mapped by their tags.
     * @param dfl The tag to apply, if this collection holds a scalar.
     * @return A stream of tagged metric-value entries.
     */
    public Stream<Entry<Tags, MetricValue>> streamAsMap(Tags dfl) {
        return values_
                .map(v -> SimpleMapEntry.create(dfl, v), Map::entrySet)
                .mapCombine(Stream::of, Collection::stream);
    }

    /**
     * Retrieve the values in this set, mapped by their tags.
     * @param dfl The tags to apply, if this collection holds a scalar.
     *   In this case, a metric value will be emitted for each tag.
     * @return A stream of tagged metric-value entries.
     */
    public Stream<Entry<Tags, MetricValue>> streamAsMap(Set<Tags> dfl) {
        return values_
                .mapCombine(v -> dfl.stream().map(tag -> SimpleMapEntry.create(tag, v)), map -> map.entrySet().stream());
    }

    /**
     * Retrieve the values in this set, mapped by their tags.
     * Scalar values will be mapped onto the empty Tags.
     * @return A stream of tagged metric-value entries.
     */
    public Stream<Entry<Tags, MetricValue>> streamAsMap() {
        return streamAsMap(Tags.EMPTY);
    }

    private static Entry<Tags, MetricValue> apply_fn_(Entry<Tags, MetricValue> entry, Function<? super MetricValue, ? extends MetricValue> fn) {
        return SimpleMapEntry.create(entry.getKey(), fn.apply(entry.getValue()));
    }
    private static Entry<Tags, MetricValue> apply_fn_optional_(Entry<Tags, MetricValue> entry, Function<? super MetricValue, Optional<? extends MetricValue>> fn) {
        return fn.apply(entry.getValue())
                .map(v -> SimpleMapEntry.create(entry.getKey(), v))
                .orElseGet(() -> SimpleMapEntry.create(entry.getKey(), MetricValue.EMPTY));
    }

    /**
     * Apply a single-argument function to the set.
     * @param fn A function that takes a TimeSeriesMetricDelta and returns a TimeSeriesMetricDelta.
     * @return The mapped TimeSeriesMetricDelta from this set.
     */
    public TimeSeriesMetricDeltaSet map(Function<? super MetricValue, ? extends MetricValue> fn) {
        return values_
                .map(
                        fn,
                        (x) -> x.entrySet().stream().map((entry) -> apply_fn_(entry, fn))
                )
                .mapCombine(TimeSeriesMetricDeltaSet::new, TimeSeriesMetricDeltaSet::new);
    }
    /**
     * Apply a single-argument function to the set.
     * @param fn A function that takes a TimeSeriesMetricDelta and returns a TimeSeriesMetricDelta.
     * @return The mapped TimeSeriesMetricDelta from this set.
     */
    public TimeSeriesMetricDeltaSet mapOptional(Function<? super MetricValue, Optional<? extends MetricValue>> fn) {
        return values_
                .map(
                        fn,
                        (x) -> x.entrySet().stream()
                                .map((entry) -> apply_fn_optional_(entry, fn))
                )
                .mapCombine(
                        opt_scalar -> opt_scalar.map(TimeSeriesMetricDeltaSet::new).orElseGet(() -> new TimeSeriesMetricDeltaSet(MetricValue.EMPTY)),
                        TimeSeriesMetricDeltaSet::new
                );
    }

    private static Stream<Entry<Tags, MetricValue>> map_(Stream<Entry<Tags, MetricValue>> s,
            MetricValue scalar,
            BiFunction<? super MetricValue, ? super MetricValue, ? extends MetricValue> fn) {
        return s.map((sval) -> apply_fn_(sval, v -> fn.apply(v, scalar)));
    }
    private static Stream<Entry<Tags, MetricValue>> map_optional_(Stream<Entry<Tags, MetricValue>> s,
            MetricValue scalar,
            BiFunction<? super MetricValue, ? super MetricValue, Optional<? extends MetricValue>> fn) {
        return s.map((sval) -> apply_fn_optional_(sval, v -> fn.apply(v, scalar)));
    }

    private static Stream<Entry<Tags, MetricValue>> map_(MetricValue scalar,
            Stream<Entry<Tags, MetricValue>> s,
            BiFunction<? super MetricValue, ? super MetricValue, ? extends MetricValue> fn) {
        return s.map((sval) -> apply_fn_(sval, v -> fn.apply(scalar, v)));
    }
    private static Stream<Entry<Tags, MetricValue>> map_optional_(MetricValue scalar,
            Stream<Entry<Tags, MetricValue>> s,
            BiFunction<? super MetricValue, ? super MetricValue, Optional<? extends MetricValue>> fn) {
        return s.map((sval) -> apply_fn_optional_(sval, v -> fn.apply(scalar, v)));
    }

    private static Stream<Entry<Tags, MetricValue>> map_(Stream<Entry<Tags, MetricValue>> x,
            Stream<Entry<Tags, MetricValue>> y,
            BiFunction<? super MetricValue, ? super MetricValue, ? extends MetricValue> fn) {
        final Map<Tags, MetricValue> y_map = y.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        return x
                .flatMap((entry) -> {
                    return Optional.ofNullable(y_map.get(entry.getKey()))
                            .map(yval -> fn.apply(entry.getValue(), yval))
                            .map(rv -> SimpleMapEntry.create(entry.getKey(), rv))
                            .map(Stream::of)
                            .orElseGet(Stream::empty);
                });
    }
    private static Stream<Entry<Tags, MetricValue>> map_optional_(Stream<Entry<Tags, MetricValue>> x,
            Stream<Entry<Tags, MetricValue>> y,
            BiFunction<? super MetricValue, ? super MetricValue, Optional<? extends MetricValue>> fn) {
        final Map<Tags, MetricValue> y_map = y.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        return x
                .flatMap((entry) -> {
                    return Optional.ofNullable(y_map.get(entry.getKey()))
                            .flatMap(yval -> fn.apply(entry.getValue(), yval))
                            .map(rv -> SimpleMapEntry.create(entry.getKey(), rv))
                            .map(Stream::of)
                            .orElseGet(Stream::empty);
                });
    }

    /**
     * Apply a scalar operation to a metric delta set.
     * @param x The scalar as the first argument to the operation.
     * @param y The set of metric deltas, upon which to apply the operation.
     * @param fn The operation to apply.
     * @return The result of the application.
     */
    public static TimeSeriesMetricDeltaSet apply(MetricValue x, TimeSeriesMetricDeltaSet y,
            BiFunction<? super MetricValue, ? super MetricValue, ? extends MetricValue> fn) {
        return y.values_
                .map(
                        (yval) -> fn.apply(x, yval),
                        (yval) -> map_(x, yval.entrySet().stream(), fn)
                )
                .mapCombine(TimeSeriesMetricDeltaSet::new, TimeSeriesMetricDeltaSet::new);
    }

    /**
     * Apply a scalar operation to a metric delta set.
     * @param x The set of metric deltas, upon which to apply the operation.
     * @param y The scalar as the second argument to the operation.
     * @param fn The operation to apply.
     * @return The result of the application.
     */
    public static TimeSeriesMetricDeltaSet apply(TimeSeriesMetricDeltaSet x, MetricValue y,
            BiFunction<? super MetricValue, ? super MetricValue, ? extends MetricValue> fn) {
        return x.values_
                .map(
                        (xval) -> fn.apply(xval, y),
                        (xval) -> map_(xval.entrySet().stream(), y, fn)
                )
                .mapCombine(TimeSeriesMetricDeltaSet::new, TimeSeriesMetricDeltaSet::new);
    }

    /**
     * Apply a 2-argument function to the set.
     *
     * Applies only exact matching tags.
     * @param fn A function that takes 2 TimeSeriesMetricDeltas and returns a TimeSeriesMetricDelta.
     * @return The mapped TimeSeriesMetricDelta from this set.
     */
    public static TimeSeriesMetricDeltaSet apply(TimeSeriesMetricDeltaSet x, TimeSeriesMetricDeltaSet y,
            BiFunction<? super MetricValue, ? super MetricValue, ? extends MetricValue> fn) {
        return x.values_
                .mapCombine(
                        xval -> y.values_
                                .map(
                                        yval -> fn.apply(xval, yval),
                                        ymap -> map_(xval, ymap.entrySet().stream(), fn)
                                )
                                .mapCombine(TimeSeriesMetricDeltaSet::new, TimeSeriesMetricDeltaSet::new),
                        xmap -> y.values_
                                .map(
                                        yval -> map_(xmap.entrySet().stream(), yval, fn),
                                        ymap -> map_(xmap.entrySet().stream(), ymap.entrySet().stream(), fn)
                                )
                                .mapCombine(TimeSeriesMetricDeltaSet::new, TimeSeriesMetricDeltaSet::new)
                );
    }

    /**
     * Reduce the tags to the specified set.
     * @param tag_names The names to retain.
     * @return A set with a reduced set of tag names.
     */
    public TimeSeriesMetricDeltaSet filterTags(Set<String> tag_names) {
        return values_
                .map(
                        Function.identity(),
                        map -> map.entrySet().stream().map(entry -> SimpleMapEntry.create(entry.getKey().filter(tag_names), entry.getValue()))
                )
                .mapCombine(TimeSeriesMetricDeltaSet::new, TimeSeriesMetricDeltaSet::new);
    }

    public Any2<MetricValue, Map<Tags, MetricValue>> getValues() {
        return values_;
    }

    @Override
    public String toString() {
        return "TimeSeriesMetricDeltaSet{" + "values_=" + values_ + '}';
    }
}
