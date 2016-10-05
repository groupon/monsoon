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
package com.groupon.lex.metrics.history.v2.tables;

import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.history.v2.xdr.group_table;
import com.groupon.lex.metrics.history.v2.xdr.metric_table;
import com.groupon.lex.metrics.history.v2.xdr.tables_metric;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import java.util.Arrays;
import static java.util.Collections.unmodifiableMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Getter;
import org.acplt.oncrpc.XdrAble;
import org.joda.time.DateTime;
import org.joda.time.Duration;

@Getter(AccessLevel.PACKAGE)
public class RTFGroupTable {
    private final DateTime begin;
    private final int[] timestamp_delta;
    private final Map<MetricName, SegmentReader<RTFMetricTable>> metrics;

    public RTFGroupTable(group_table input, DateTime begin, DictionaryDelta dictionary, SegmentReader.Factory<XdrAble> segmentFactory) {
        this.begin = begin;
        timestamp_delta = input.timestamp_delta;
        metrics = unmodifiableMap(metricsMap(input.metric_tbl, begin, dictionary, segmentFactory));
    }

    private static Map<MetricName, SegmentReader<RTFMetricTable>> metricsMap(tables_metric tmArray[], DateTime begin, DictionaryDelta dictionary, SegmentReader.Factory<XdrAble> segmentFactory) {
        return Arrays.stream(tmArray)
                .collect(Collectors.toMap(
                        tm -> MetricName.valueOf(dictionary.getPath(tm.metric_ref)),
                        tm -> segmentFactory.get(metric_table::new, new FilePos(tm.pos))
                                .map(mt -> new RTFMetricTable(mt, begin, dictionary))
                                .share()));
    }

    public boolean isPresent(long ts) {
        if (ts < begin.getMillis()) return false;
        final long tsOffset_long = ts - begin.getMillis();
        if (tsOffset_long > Integer.MAX_VALUE) return false;

        return Arrays.binarySearch(timestamp_delta, (int)tsOffset_long) >= 0;
    }

    public Stream<DateTime> getTimestamps() {
        return Arrays.stream(timestamp_delta)
                .mapToObj(Duration::new)
                .map(begin::plus);
    }

    public boolean contains(DateTime ts) {
        if (ts.isBefore(begin)) return false;
        final long tsOffset_long = new Duration(begin, ts).getMillis();
        if (tsOffset_long > Integer.MAX_VALUE) return false;

        final int tsOffset = (int)tsOffset_long;
        return Arrays.binarySearch(timestamp_delta, tsOffset) >= 0;
    }

    public DateTime minTimestamp() {
        if (timestamp_delta.length == 0) return null;
        return begin.plus(new Duration(timestamp_delta[0]));
    }

    public DateTime maxTimestamp() {
        if (timestamp_delta.length == 0) return null;
        return begin.plus(new Duration(timestamp_delta[timestamp_delta.length - 1]));
    }

    public Set<MetricName> getMetricNames() {
        return metrics.keySet();
    }

    public SegmentReader<RTFMetricTable> getMetric(MetricName name) {
        return metrics.get(name);
    }
}
