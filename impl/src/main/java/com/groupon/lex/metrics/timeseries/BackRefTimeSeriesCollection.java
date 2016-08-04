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
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import gnu.trove.map.hash.THashMap;
import java.util.Collection;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.Set;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

/**
 * A TimeSeriesCollection that allows updating with a new scrape set.
 *
 * It remembers older values that are not present after merging in a new set,
 * so that rate queries and back-referencing queries can still get data.
 */
public class BackRefTimeSeriesCollection implements TimeSeriesCollection {
    public static final Duration MAX_AGE = Duration.standardMinutes(30);
    private DateTime timestamp_;
    private final Map<GroupName, TimeSeriesValue> data_ = new THashMap<>();
    private final Map<SimpleGroupPath, Set<TimeSeriesValue>> data_by_path_ = new THashMap<>();

    private void add_(TimeSeriesValue tsv) {
        final GroupName name = tsv.getGroup();
        final SimpleGroupPath path = name.getPath();

        final Optional<TimeSeriesValue> removed = Optional.ofNullable(data_.put(name, tsv));
        removed.ifPresent(r -> {
            final boolean is_removed = data_by_path_.get(path).remove(r);
            assert(is_removed);
        });
        data_by_path_.computeIfAbsent(path, (p) -> new HashSet<>())
                .add(tsv);
    }

    private void remove_by_path_only_(TimeSeriesValue tsv) {
        final SimpleGroupPath path = tsv.getGroup().getPath();

        Set<TimeSeriesValue> set = data_by_path_.get(path);
        final boolean is_removed = set.remove(tsv);
        assert(is_removed);
        if (set.isEmpty())
            data_by_path_.remove(path);
    }

    public BackRefTimeSeriesCollection() {
        this(new DateTime(DateTimeZone.UTC));
    }

    public BackRefTimeSeriesCollection(DateTime initial) {
        timestamp_ = requireNonNull(initial);
    }

    private BackRefTimeSeriesCollection(BackRefTimeSeriesCollection o) {
        timestamp_ = o.getTimestamp();
        data_.putAll(o.data_);
        data_by_path_.putAll(o.data_by_path_);
    }

    @Override
    public TimeSeriesCollection add(TimeSeriesValue tsv) {
        throw new UnsupportedOperationException("Back-reference is not mutable");
    }

    @Override
    public TimeSeriesCollection renameGroup(GroupName oldname, GroupName newname) {
        throw new UnsupportedOperationException("Back-reference is not mutable");
    }

    @Override
    public TimeSeriesCollection addMetrics(GroupName group, Map<MetricName, MetricValue> metrics) {
        throw new UnsupportedOperationException("Back-reference is not mutable");
    }

    @Override
    public DateTime getTimestamp() {
        return timestamp_;
    }

    @Override
    public boolean isEmpty() {
        return data_.isEmpty();
    }

    @Override
    public Set<GroupName> getGroups() {
        return unmodifiableSet(new HashSet<>(data_.keySet()));  // Copy of the keyset, since Map.keySet is a view.
    }

    @Override
    public Set<SimpleGroupPath> getGroupPaths() {
        return unmodifiableSet(new HashSet<>(data_by_path_.keySet()));
    }

    @Override
    public Collection<TimeSeriesValue> getTSValues() {
        return unmodifiableCollection(data_.values());
    }

    @Override
    public TimeSeriesValueSet getTSValue(SimpleGroupPath name) {
        final SimpleGroupPath sname;
        if (name instanceof SimpleGroupPath)
            sname = (SimpleGroupPath)name;
        else
            sname = SimpleGroupPath.valueOf(name.getPath());

        return Optional.ofNullable(data_by_path_.get(sname))
                .map(Collection::stream)
                .map(TimeSeriesValueSet::new)
                .orElse(TimeSeriesValueSet.EMPTY);
    }

    @Override
    public Optional<TimeSeriesValue> get(GroupName name) {
        return Optional.ofNullable(data_.get(name));
    }

    public BackRefTimeSeriesCollection merge(DateTime timestamp, Collection<TimeSeriesValue> values) {
        requireNonNull(values);
        timestamp_ = requireNonNull(timestamp);

        // Remove entries that are now considered expired.
        final DateTime expire = timestamp_.minus(MAX_AGE);
        Iterator<TimeSeriesValue> iter = data_.values().iterator();
        while (iter.hasNext()) {
            TimeSeriesValue tsv = iter.next();
            if (tsv.getTimestamp().isBefore(expire)) {
                iter.remove();
                remove_by_path_only_(tsv);
            }
        }

        // Add new entries.
        values.forEach(this::add_);
        return this;
    }

    public BackRefTimeSeriesCollection merge(MutableTimeSeriesCollection c) {
        return merge(c.getTimestamp(), c.getData().values());
    }

    @Override
    public BackRefTimeSeriesCollection clone() {
        return new BackRefTimeSeriesCollection(this);
    }

    @Override
    public String toString() {
        return "BackRefTimeSeriesCollection{" + timestamp_ + ", " + data_.values() + '}';
    }
}
