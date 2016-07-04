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
import java.util.Collection;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author ariane
 */
public class MutableTimeSeriesCollection implements TimeSeriesCollection, Cloneable {
    private static final Logger LOG = Logger.getLogger(MutableTimeSeriesCollection.class.getName());

    private DateTime timestamp_;
    private final Map<GroupName, MutableTimeSeriesValue> data_ = new HashMap<>();
    private final Map<SimpleGroupPath, Set<MutableTimeSeriesValue>> data_by_path_ = new HashMap<>();

    private void add_(MutableTimeSeriesValue tsv) {
        final GroupName name = tsv.getGroup();
        final SimpleGroupPath path = name.getPath();

        final Optional<MutableTimeSeriesValue> removed = Optional.ofNullable(data_.put(name, tsv));
        removed.ifPresent(r -> {
            final boolean is_removed = data_by_path_.get(path).remove(r);
            assert(is_removed);
        });
        data_by_path_.computeIfAbsent(path, (p) -> new HashSet<>())
                .add(tsv);
    }

    private Optional<MutableTimeSeriesValue> remove_(GroupName name) {
        final SimpleGroupPath path = name.getPath();

        final Optional<MutableTimeSeriesValue> removed = Optional.ofNullable(data_.remove(name));
        removed.ifPresent(r -> {
            Set<MutableTimeSeriesValue> set = data_by_path_.get(path);
            final boolean is_removed = set.remove(r);
            assert(is_removed);
            if (set.isEmpty())
                data_by_path_.remove(path);
        });
        return removed;
    }

    public MutableTimeSeriesCollection(DateTime timestamp, Stream<? extends TimeSeriesValue> values) {
        timestamp_ = requireNonNull(timestamp);
        values.forEach(this::add);
    }

    public MutableTimeSeriesCollection(DateTime timestamp) {
        timestamp_ = requireNonNull(timestamp);
    }

    public MutableTimeSeriesCollection() {
        this(new DateTime(DateTimeZone.UTC));
    }

    @Override
    public MutableTimeSeriesCollection add(TimeSeriesValue tsv) {
        return add(new MutableTimeSeriesValue(tsv));
    }

    public MutableTimeSeriesCollection add(MutableTimeSeriesValue tsv) {
        add_(tsv);
        return this;
    }

    @Override
    public TimeSeriesCollection renameGroup(GroupName oldname, GroupName newname) {
        LOG.log(Level.FINE, "renaming {0} => {1}", new Object[]{oldname, newname});
        remove_(oldname)
                .ifPresent(tsv -> {
                    tsv.setGroup(newname);
                    add_(tsv);
                });
        return this;
    }

    @Override
    public TimeSeriesCollection addMetric(GroupName group, MetricName metric, MetricValue value) {
        return addMetrics(group, singletonMap(metric, value));
    }

    @Override
    public TimeSeriesCollection addMetrics(GroupName group, Map<MetricName, MetricValue> metrics) {
        final Optional<MutableTimeSeriesValue> opt_tsv = remove_(group);  // Unlink, since we may alter the hash bucket.
        if (!opt_tsv.isPresent()) {
            add_(new MutableTimeSeriesValue(getTimestamp(), group, metrics));
        } else {
            final MutableTimeSeriesValue tsv = opt_tsv.get();
            tsv.addMetrics(metrics);
            add_(tsv);
        }
        return this;
    }

    @Override
    public DateTime getTimestamp() { return timestamp_; }

    public MutableTimeSeriesCollection setTimestamp(DateTime timestamp) {
        timestamp_ = requireNonNull(timestamp);
        return this;
    }

    @Override
    public boolean isEmpty() { return data_.isEmpty(); }

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
        return Optional.ofNullable(data_by_path_.get(name))
                .map(Collection::stream)
                .map(TimeSeriesValueSet::new)
                .orElse(TimeSeriesValueSet.EMPTY);
    }

    @Override
    public Optional<TimeSeriesValue> get(GroupName name) {
        return Optional.ofNullable(data_.get(name));
    }

    public Map<GroupName, TimeSeriesValue> getData() {
        return unmodifiableMap(data_);
    }

    public MutableTimeSeriesCollection clear(DateTime timestamp) {
        setTimestamp(timestamp);
        data_.clear();
        data_by_path_.clear();
        return this;
    }

    @Override
    public MutableTimeSeriesCollection clone() {
        return new MutableTimeSeriesCollection(getTimestamp(), getData().values().stream());
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + Objects.hashCode(this.timestamp_);
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
        final MutableTimeSeriesCollection other = (MutableTimeSeriesCollection) obj;
        if (!Objects.equals(this.timestamp_, other.timestamp_)) {
            return false;
        }
        if (!Objects.equals(this.data_, other.data_)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "MutableTimeSeriesCollection{" + timestamp_ + ", " + data_.values() + '}';
    }
}
