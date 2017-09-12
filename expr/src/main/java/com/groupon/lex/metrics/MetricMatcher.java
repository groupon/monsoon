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

import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.expression.Context;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

/**
 *
 * @author ariane
 */
@RequiredArgsConstructor
@EqualsAndHashCode
@Getter
public class MetricMatcher {
    private static final Logger LOG = Logger.getLogger(MetricMatcher.class.getName());
    @NonNull
    private final PathMatcher groups;
    @NonNull
    private final PathMatcher metric;

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

    public boolean match(SimpleGroupPath grp) {
        return groups.match(grp.getPath());
    }

    public boolean match(MetricName m) {
        return metric.match(m.getPath());
    }

    /**
     * Create a stream of TimeSeriesMetricDeltas with values.
     */
    public Stream<Entry<MatchedName, MetricValue>> filter(Context t) {
        return t.getTSData().getCurrentCollection().get(this::match, x -> true).stream()
                .flatMap(this::filterMetricsInTsv);
    }

    public StringBuilder configString() {
        return groups.configString()
                .append(' ')
                .append(metric.configString());
    }

    @Override
    public String toString() {
        return "MetricMatcher{" + configString().toString() + "}";
    }

    private Stream<Entry<MatchedName, MetricValue>> filterMetricsInTsv(TimeSeriesValue tsv) {
        return tsv.getMetrics().entrySet().stream()
                .filter(entry -> match(entry.getKey()))
                .map(metricEntry -> SimpleMapEntry.create(new MatchedName(tsv, metricEntry.getKey()), metricEntry.getValue()));
    }
}
