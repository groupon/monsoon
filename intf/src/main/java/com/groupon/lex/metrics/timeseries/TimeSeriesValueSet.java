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

import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import java.util.Collection;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.unmodifiableMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * A collection of timeseries values.
 * @author ariane
 */
@EqualsAndHashCode
@ToString
public class TimeSeriesValueSet {
    public static final TimeSeriesValueSet EMPTY = new TimeSeriesValueSet(EMPTY_LIST);
    private Map<SimpleGroupPath, List<TimeSeriesValue>> tsv_;

    public TimeSeriesValueSet(Stream<? extends TimeSeriesValue> tsv) {
        tsv_ = tsv
                .map(x -> {
                    final TimeSeriesValue r = x;  // Implicit cast
                    return r;
                })
                .collect(Collectors.groupingBy(v -> v.getGroup().getPath()));
    }

    public TimeSeriesValueSet(Collection<? extends TimeSeriesValue> tsv) {
        this(tsv.stream());
    }

    public boolean isEmpty() { return tsv_.isEmpty(); }
    public Map<SimpleGroupPath, Collection<TimeSeriesValue>> asMap() { return unmodifiableMap(tsv_); }
    public Set<SimpleGroupPath> getPaths() { return asMap().keySet(); }

    public Stream<TimeSeriesValue> stream() {
        return tsv_.values().stream()
                .flatMap(Collection::stream);
    }

    public Stream<TimeSeriesValue> stream(SimpleGroupPath path) {
        return Optional.ofNullable(tsv_.get(path))
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }

    public TimeSeriesMetricDeltaSet findMetric(MetricName metric) {
        return new TimeSeriesMetricDeltaSet(stream()
                .map(tsv -> tsv.findMetric(metric).map(v -> SimpleMapEntry.create(tsv.getTags(), v)))
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .map(e -> SimpleMapEntry.create(e.getKey(), e.getValue())));
    }
}
