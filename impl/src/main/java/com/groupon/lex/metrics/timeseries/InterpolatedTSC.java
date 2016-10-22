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
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.lib.LazyMap;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import gnu.trove.set.hash.THashSet;
import static java.lang.Math.max;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public class InterpolatedTSC extends AbstractTimeSeriesCollection implements TimeSeriesCollection {
    private static final Logger LOG = Logger.getLogger(InterpolatedTSC.class.getName());
    /**
     * TimeSeriesCollections used for interpolation.
     */
    private final List<TimeSeriesCollection> backward, forward;
    /**
     * Current TimeSeriesCollection.
     */
    private final TimeSeriesCollection current;
    /**
     * TimeSeriesValues.
     */
    private final Map<GroupName, TimeSeriesValue> interpolatedTsvMap;

    public InterpolatedTSC(TimeSeriesCollection current, Collection<TimeSeriesCollection> backward, Collection<TimeSeriesCollection> forward) {
        this.current = current;
        this.backward = unmodifiableList(new ArrayList<>(backward));
        this.forward = unmodifiableList(new ArrayList<>(forward));
        this.interpolatedTsvMap = new LazyMap<>(this::interpolateTSV, calculateNames(this.current, this.backward, this.forward));

        validate();
    }

    /**
     * Check the forward and backward invariants.
     */
    private void validate() {
        try {
            DateTime ts;

            ts = current.getTimestamp();
            for (TimeSeriesCollection b : backward) {
                if (b.getTimestamp().isAfter(ts))
                    throw new IllegalArgumentException("backwards collection must be before current and be ordered in reverse chronological order");
                ts = b.getTimestamp();
            }

            ts = current.getTimestamp();
            for (TimeSeriesCollection f : forward) {
                if (f.getTimestamp().isBefore(ts))
                    throw new IllegalArgumentException("forwards collection must be after current and be ordered in chronological order");
                ts = f.getTimestamp();
            }
        } catch (IllegalArgumentException ex) {
            LOG.log(Level.SEVERE, "programmer error in creating interpolated TimeSeriesCollection", ex);
            final List<DateTime> backward_ts = backward.stream().map(TimeSeriesCollection::getTimestamp).collect(Collectors.toList());
            final List<DateTime> forward_ts = forward.stream().map(TimeSeriesCollection::getTimestamp).collect(Collectors.toList());
            LOG.log(Level.INFO, "current = {0}, backward = {1}, forward = {2}", new Object[]{current.getTimestamp(), backward_ts, forward_ts});
            throw ex;
        }
    }

    @Override
    public DateTime getTimestamp() {
        return current.getTimestamp();
    }

    @Override
    public boolean isEmpty() {
        return interpolatedTsvMap.isEmpty() && current.isEmpty();
    }

    @Override
    public Set<GroupName> getGroups() {
        Set<GroupName> groups = new HashSet<>(current.getGroups());
        groups.addAll(interpolatedTsvMap.keySet());
        return groups;
    }

    @Override
    public Set<SimpleGroupPath> getGroupPaths() {
        Set<SimpleGroupPath> paths = new HashSet<>(current.getGroupPaths());
        interpolatedTsvMap.keySet().stream()
                .map(GroupName::getPath)
                .forEach(paths::add);
        return paths;
    }

    @Override
    public Collection<TimeSeriesValue> getTSValues() {
        List<TimeSeriesValue> tsValues = new ArrayList<>(current.getTSValues());
        tsValues.addAll(interpolatedTsvMap.values());
        return tsValues;
    }

    @Override
    public TimeSeriesValueSet getTSValue(SimpleGroupPath name) {
        Stream<TimeSeriesValue> interpolatedValues = interpolatedTsvMap.keySet().stream()
                .filter(g -> g.getPath().equals(name))
                .map(interpolatedTsvMap::get);
        Stream<TimeSeriesValue> currentValues = current.getTSValue(name).stream();
        return new TimeSeriesValueSet(Stream.concat(interpolatedValues, currentValues));
    }

    @Override
    public Optional<TimeSeriesValue> get(GroupName name) {
        final TimeSeriesValue interpolated = interpolatedTsvMap.get(name);
        if (interpolated != null) return Optional.of(interpolated);
        return current.get(name);
    }

    /**
     * Calculate all names that can be interpolated. The returned set will not
     * have any names present in the current collection.
     */
    private static Set<GroupName> calculateNames(TimeSeriesCollection current, Collection<TimeSeriesCollection> backward, Collection<TimeSeriesCollection> forward) {
        final Set<GroupName> names = backward.stream()
                .map(TimeSeriesCollection::getGroups)
                .flatMap(Collection::stream)
                .collect(Collectors.toCollection(THashSet::new));
        names.retainAll(forward.stream()
                .map(TimeSeriesCollection::getGroups)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet()));
        names.removeAll(current.getGroups());
        return names;
    }

    /**
     * Interpolates a group name, based on the most recent backward and oldest
     * forward occurence.
     *
     * @param name The name of the group to interpolate.
     * @return The interpolated name of the group.
     */
    private TimeSeriesValue interpolateTSV(GroupName name) {
        final Map.Entry<DateTime, TimeSeriesValue> backTSV = findName(backward, name),
                forwTSV = findName(forward, name);

        final long backMillis = max(new Duration(backTSV.getKey(), getTimestamp()).getMillis(), 0),
                forwMillis = max(new Duration(getTimestamp(), forwTSV.getKey()).getMillis(), 0);
        final double totalMillis = forwMillis + backMillis;
        final double backWeight = forwMillis / totalMillis;
        final double forwWeight = backMillis / totalMillis;

        return new InterpolatedTSV(name, backTSV.getValue().getMetrics(), forwTSV.getValue().getMetrics(), backWeight, forwWeight);
    }

    /**
     * Finds the first resolution of name in the given TimeSeriesCollections.
     * The cache is used and updated to skip the linear search phase.
     *
     * @param c TimeSeriesCollection instances in which to search.
     * @param name The searched for name. Note that the name must be present in
     * the collection.
     * @param cache Cache used and updated during lookups, to skip the linear
     * search.
     * @return The first TimeSeriesValue in the list of TSCollections with the
     * given name.
     * @throws IllegalStateException if the name was not found.
     */
    private static Map.Entry<DateTime, TimeSeriesValue> findName(List<TimeSeriesCollection> c, GroupName name) {
        ListIterator<TimeSeriesCollection> iter = c.listIterator();
        while (iter.hasNext()) {
            final int idx = iter.nextIndex();
            final TimeSeriesCollection tsdata = iter.next();

            final Optional<TimeSeriesValue> found = tsdata.get(name);
            if (found.isPresent())
                return SimpleMapEntry.create(tsdata.getTimestamp(), found.get());
        }

        throw new IllegalStateException("name not present in list of time series collections");
    }

    @Getter
    private static class InterpolatedTSV extends AbstractTimeSeriesValue implements TimeSeriesValue {
        private final GroupName group;
        private final Map<MetricName, MetricValue> backward, forward;
        private final double backWeight, forwWeight;
        private final Map<MetricName, MetricValue> metrics;

        public InterpolatedTSV(
                GroupName group,
                Map<MetricName, MetricValue> backward,
                Map<MetricName, MetricValue> forward,
                double backWeight,
                double forwWeight) {
            this.group = group;
            this.backward = backward;
            this.forward = forward;
            this.backWeight = backWeight;
            this.forwWeight = forwWeight;

            // Compute the intersection of metric names.
            final Set<MetricName> names = new THashSet<>(backward.keySet());
            names.retainAll(forward.keySet());

            this.metrics = unmodifiableMap(new LazyMap<>(key -> interpolate(backward.get(key), forward.get(key)), names));
        }

        @Override
        public InterpolatedTSV clone() {
            return this;  // Immutable.
        }

        private MetricValue interpolate(MetricValue a, MetricValue b) {
            if ((a.getBoolValue() != null || a.getIntValue() != null || a.getFltValue() != null)
                    && (b.getBoolValue() != null || b.getIntValue() != null || b.getFltValue() != null))
                return MetricValue.fromDblValue(backWeight * a.value().get().doubleValue() + forwWeight * b.value().get().doubleValue());

            if (a.getStrValue() != null && b.getStrValue() != null)
                return a;

            if (a.getHistValue() != null && b.getHistValue() != null) {
                return MetricValue.fromHistValue(Histogram.add(
                        Histogram.multiply(a.getHistValue(), backWeight),
                        Histogram.multiply(b.getHistValue(), forwWeight)));
            }

            // Mismatched types, return empty metric value.
            return MetricValue.EMPTY;
        }
    }
}
