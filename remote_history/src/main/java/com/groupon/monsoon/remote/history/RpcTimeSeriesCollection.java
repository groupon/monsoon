/*
 * Copyright (c) 2016, Ariane van der Steldt
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
package com.groupon.monsoon.remote.history;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import gnu.trove.map.hash.THashMap;
import java.util.Collection;
import static java.util.Collections.unmodifiableCollection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.joda.time.DateTime;

@EqualsAndHashCode
@ToString
public class RpcTimeSeriesCollection implements TimeSeriesCollection {
    private final DateTime timestamp_;
    private final Map<SimpleGroupPath, Map<Tags, TimeSeriesValue>> path_map_;

    private static <T> BinaryOperator<T> throwing_merger_() {
        return (x, y) -> { throw new IllegalStateException("duplicate key " + x); };
    }

    /** HashMap constructor, so we can create hashmaps with an altered load factor. */
    private static <K, V> Supplier<Map<K, V>> hashmap_constructor_() {
        return () -> new THashMap<K, V>(1, 1);
    }

    public RpcTimeSeriesCollection(DateTime timestamp, Stream<TimeSeriesValue> tsv) {
        timestamp_ = timestamp;
        path_map_ = tsv.collect(
                Collectors.groupingBy(
                        v -> v.getGroup().getPath(),
                        hashmap_constructor_(),
                        Collectors.toMap(
                                v -> v.getGroup().getTags(),
                                Function.identity(),
                                throwing_merger_(),
                                hashmap_constructor_())));
    }

    @Override
    public TimeSeriesCollection add(TimeSeriesValue tsv) {
        throw new UnsupportedOperationException("Immutable.");
    }

    @Override
    public TimeSeriesCollection renameGroup(GroupName oldname, GroupName newname) {
        throw new UnsupportedOperationException("Immutable.");
    }

    @Override
    public TimeSeriesCollection addMetrics(GroupName group, Map<MetricName, MetricValue> metrics) {
        throw new UnsupportedOperationException("Immutable.");
    }

    @Override
    public DateTime getTimestamp() {
        return timestamp_;
    }

    @Override
    public boolean isEmpty() {
        return path_map_.isEmpty();
    }

    @Override
    public Set<GroupName> getGroups() {
        return path_map_.values().stream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .map(TimeSeriesValue::getGroup)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<SimpleGroupPath> getGroupPaths() {
        return path_map_.keySet();
    }

    @Override
    public Collection<TimeSeriesValue> getTSValues() {
        return unmodifiableCollection(path_map_.values().stream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));
    }

    @Override
    public TimeSeriesValueSet getTSValue(SimpleGroupPath name) {
        return Optional.ofNullable(path_map_.get(name))
                .map(Map::values)
                .map(Collection::stream)
                .map(TimeSeriesValueSet::new)
                .orElse(TimeSeriesValueSet.EMPTY);
    }

    @Override
    public Optional<TimeSeriesValue> get(GroupName name) {
        return Optional.ofNullable(path_map_.get(name.getPath()))
                .flatMap(tag_map -> Optional.ofNullable(tag_map.get(name.getTags())));
    }

    @Override
    public TimeSeriesCollection clone() {
        return this;  // Immutable
    }
}
