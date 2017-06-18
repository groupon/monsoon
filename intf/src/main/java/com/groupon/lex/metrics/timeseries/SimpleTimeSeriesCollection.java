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

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.SimpleGroupPath;
import gnu.trove.map.hash.THashMap;
import java.util.Collection;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import org.joda.time.DateTime;

/**
 * A simple time series collection. This implementation uses trove hashmap to
 * minimize memory usage.
 *
 * @author ariane
 */
public class SimpleTimeSeriesCollection extends AbstractTimeSeriesCollection {
    @Getter
    private final DateTime timestamp;
    private final Map<GroupName, TimeSeriesValue> groupMap;
    private final Map<SimpleGroupPath, List<TimeSeriesValue>> pathMap;

    private static BinaryOperator<TimeSeriesValue> throwing_merger_() {
        return (x, y) -> {
            throw new IllegalArgumentException("duplicate group " + x.getGroup());
        };
    }

    /**
     * HashMap constructor, so we can create hashmaps with an altered load
     * factor.
     */
    private static <K, V> Supplier<Map<K, V>> hashmap_constructor_() {
        return () -> new THashMap<>(1, 1);
    }

    /**
     * Construct a time series collection.
     *
     * @param timestamp The timestamp of the collection.
     * @param tsv A stream of values for the collection. The values stream may
     * not have duplicate group names.
     * @throws IllegalArgumentException if the time series values contain
     * duplicate group names.
     */
    public SimpleTimeSeriesCollection(@NonNull DateTime timestamp, @NonNull Stream<? extends TimeSeriesValue> tsv) {
        this.timestamp = timestamp;
        groupMap = tsv.collect(Collectors.toMap(TimeSeriesValue::getGroup, v -> v, throwing_merger_(), hashmap_constructor_()));
        pathMap = groupMap.values().stream()
                .collect(Collectors.groupingBy(v -> v.getGroup().getPath(), hashmap_constructor_(), Collectors.toList()));
    }

    /**
     * Construct a time series collection.
     *
     * @param timestamp The timestamp of the collection.
     * @param tsv A collection of values for the collection. The values stream
     * may not have duplicate group names.
     * @throws IllegalArgumentException if the time series values contain
     * duplicate group names.
     */
    public SimpleTimeSeriesCollection(@NonNull DateTime timestamp, @NonNull Collection<? extends TimeSeriesValue> tsv) {
        this(timestamp, tsv.stream());
    }

    @Override
    public boolean isEmpty() {
        return pathMap.isEmpty();
    }

    @Override
    public Set<GroupName> getGroups(Predicate<? super GroupName> filter) {
        return groupMap.keySet().stream()
                .filter(filter)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<SimpleGroupPath> getGroupPaths() {
        return unmodifiableSet(pathMap.keySet());
    }

    @Override
    public Collection<TimeSeriesValue> getTSValues() {
        return unmodifiableCollection(groupMap.values());
    }

    @Override
    public TimeSeriesValueSet getTSValue(SimpleGroupPath name) {
        final List<TimeSeriesValue> tsvSet = pathMap.get(name);
        if (tsvSet == null) return TimeSeriesValueSet.EMPTY;
        return new TimeSeriesValueSet(tsvSet);
    }

    @Override
    public Optional<TimeSeriesValue> get(GroupName name) {
        return Optional.ofNullable(groupMap.get(name));
    }
}
