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
package com.groupon.lex.metrics.config.parser;

import com.groupon.lex.metrics.GroupGenerator;
import static com.groupon.lex.metrics.GroupGenerator.successResult;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricGroup;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleMetric;
import com.groupon.lex.metrics.SimpleMetricGroup;
import com.groupon.lex.metrics.timeseries.AlertState;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public class ReplayCollector implements GroupGenerator {
    public static class DataPointIdentifier {
        private final GroupName group_;
        private final MetricName metric_;

        public DataPointIdentifier(GroupName group, String metric) {
            this(group, new MetricName(requireNonNull(metric)));
        }

        public DataPointIdentifier(GroupName group, MetricName metric) {
            group_ = requireNonNull(group);
            metric_ = requireNonNull(metric);
        }

        public GroupName getGroup() {
            return group_;
        }

        public MetricName getMetric() {
            return metric_;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 53 * hash + Objects.hashCode(this.group_);
            hash = 53 * hash + Objects.hashCode(this.metric_);
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
            final DataPointIdentifier other = (DataPointIdentifier) obj;
            if (!Objects.equals(this.group_, other.group_)) {
                return false;
            }
            if (!Objects.equals(this.metric_, other.metric_)) {
                return false;
            }
            return true;
        }
    }

    private static class DataPointIterator implements Iterator<Collection<MetricGroup>> {
        private final Map<DataPointIdentifier, Iterator<Optional<MetricValue>>> data_ = new HashMap<>();

        public DataPointIterator(Map<DataPointIdentifier, Stream<Optional<MetricValue>>> input) {
            input.forEach((DataPointIdentifier key, Stream<Optional<MetricValue>> data) -> {
                        Iterator<Optional<MetricValue>> data_iterator = data.iterator();
                        if (data_iterator.hasNext())
                            data_.put(key, data_iterator);
                    });
        }

        @Override
        public boolean hasNext() {
            return !data_.isEmpty();
        }

        @Override
        public Collection<MetricGroup> next() {
            Map<GroupName, SimpleMetricGroup> result = new HashMap<>();

            data_.forEach((id, value_iter) -> {
                assert(value_iter.hasNext());
                value_iter.next().ifPresent((v) -> {
                    SimpleMetricGroup smg = result.computeIfAbsent(id.getGroup(), SimpleMetricGroup::new);
                    smg.add(new SimpleMetric(id.getMetric(), v));
                });
            });
            data_.values().removeIf((iter) -> !iter.hasNext());

            return unmodifiableCollection(result.values());
        }
    }

    public static class DataPointStream implements Iterable<Optional<MetricValue>> {
        private final DataPointIdentifier identifier_;
        private final List<Optional<MetricValue>> stream_ = new ArrayList<>();

        public DataPointStream(DataPointIdentifier identifier, Iterator<Optional<MetricValue>> stream) {
            identifier_ = identifier;
            while (stream.hasNext())
                stream_.add(requireNonNull(stream.next()));
        }

        public DataPointStream(DataPointIdentifier identifier, Iterable<Optional<MetricValue>> stream) {
            this(identifier, stream.iterator());
        }

        public DataPointStream(DataPointIdentifier identifier, Stream<Optional<MetricValue>> stream) {
            this(identifier, stream.iterator());
        }

        public DataPointStream(DataPointIdentifier identifier) {
            this(identifier, EMPTY_LIST);
        }

        public DataPointIdentifier getIdentifier() { return identifier_; }

        @Override
        public Iterator<Optional<MetricValue>> iterator() {
            return unmodifiableList(stream_).iterator();
        }

        public Stream<Optional<MetricValue>> asStream() {
            return stream_.stream();
        }

        public void add(Optional<MetricValue> value) { stream_.add(requireNonNull(value)); }

        public void addAll(Iterable<Optional<MetricValue>> values) {
            values.forEach(this::add);
        }
    }

    public static class AlertStream implements Iterable<AlertState> {
        private final GroupName identifier_;
        private final List<AlertState> stream_ = new ArrayList<>();

        public AlertStream(GroupName identifier, Iterator<AlertState> stream) {
            identifier_ = identifier;
            while (stream.hasNext())
                stream_.add(requireNonNull(stream.next()));
        }

        public AlertStream(GroupName identifier, Iterable<AlertState> stream) {
            this(identifier, stream.iterator());
        }

        public AlertStream(GroupName identifier, Stream<AlertState> stream) {
            this(identifier, stream.iterator());
        }

        public AlertStream(GroupName identifier) {
            this(identifier, EMPTY_LIST);
        }

        public GroupName getIdentifier() { return identifier_; }

        @Override
        public Iterator<AlertState> iterator() {
            return unmodifiableList(stream_).iterator();
        }

        public Stream<AlertState> asStream() {
            return stream_.stream();
        }

        public void add(AlertState value) { stream_.add(requireNonNull(value)); }

        public void addAll(Iterable<AlertState> values) {
            values.forEach(this::add);
        }
    }

    private final Iterator<Collection<MetricGroup>> data_iter_;

    public boolean hasNext() {
        return data_iter_.hasNext();
    }

    public ReplayCollector(Iterator<DataPointStream> dps_iter) {
        Map<DataPointIdentifier, Stream<Optional<MetricValue>>> transformed = new HashMap<>();
        while (dps_iter.hasNext()) {
            final DataPointStream dps = dps_iter.next();
            transformed.put(dps.getIdentifier(), dps.asStream());
        }
        data_iter_ = new DataPointIterator(transformed);
    }

    public ReplayCollector(Stream<DataPointStream> dps_stream) {
        this(dps_stream.iterator());
    }

    public ReplayCollector(Iterable<DataPointStream> dps_iterable) {
        this(dps_iterable.iterator());
    }

    public ReplayCollector(DataPointStream... dps_array) {
        this(Arrays.asList(dps_array));
    }

    @Override
    public GroupCollection getGroups() {
        return successResult(data_iter_.next());
    }
}
