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
package com.groupon.lex.metrics;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.expression.Context;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;

/**
 *
 * @author ariane
 */
public class MetricMatcher {
    private static final Logger LOG = Logger.getLogger(MetricMatcher.class.getName());
    private final PathMatcher groups_;
    private final PathMatcher metric_;
    private final LoadingCache<Set<MetricName>, Collection<MetricName>> metric_match_cache_;

    @Value
    public static final class MatchedName {
        private final TimeSeriesValue matchedGroup;
        private final MetricName metric;

        public GroupName getGroup() {
            return matchedGroup.getGroup();
        }

        public Tags getTags() {
            return getGroup().getTags();
        }
    }

    public MetricMatcher(PathMatcher groups, PathMatcher metric) {
        groups_ = requireNonNull(groups);
        metric_ = requireNonNull(metric);
        metric_match_cache_ = CacheBuilder.newBuilder()
                .softValues()
                .build(CacheLoader.from((Set<MetricName> names) -> names.stream().filter(this::match).collect(Collectors.toList())));
    }

    public boolean match(SimpleGroupPath grp) {
        return groups_.match(grp.getPath());
    }

    public boolean match(MetricName metric) {
        return metric_.match(metric.getPath());
    }

    /**
     * Create a stream of TimeSeriesMetricDeltas with values.
     */
    public Stream<Entry<MatchedName, MetricValue>> filter(Context t) {
        return t.getTSData().getCurrentCollection().get(this::match, x -> true).stream()
                .flatMap(tsv -> {
                    return metric_match_cache_.getUnchecked(new HashSet<>(tsv.getMetrics().keySet())).stream()
                            .map(metric_name -> {
                                return SimpleMapEntry.create(
                                        new MatchedName(tsv, metric_name),
                                        tsv.findMetric(metric_name).orElseThrow(() -> new IllegalStateException("resolved metric name was not present")));
                            });
                });
    }

    public StringBuilder configString() {
        return groups_.configString()
                .append(' ')
                .append(metric_.configString());
    }

    @Override
    public String toString() {
        return "MetricMatcher{" + configString().toString() + "}";
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 29 * hash + Objects.hashCode(this.groups_);
        hash = 29 * hash + Objects.hashCode(this.metric_);
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
        final MetricMatcher other = (MetricMatcher) obj;
        if (!Objects.equals(this.groups_, other.groups_)) {
            return false;
        }
        if (!Objects.equals(this.metric_, other.metric_)) {
            return false;
        }
        return true;
    }
}
