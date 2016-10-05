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
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.group_table;
import com.groupon.lex.metrics.history.v2.xdr.metric_table;
import com.groupon.lex.metrics.history.v2.xdr.tables_metric;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import java.util.Arrays;
import static java.util.Collections.unmodifiableMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import org.acplt.oncrpc.XdrAble;

@Getter(AccessLevel.PACKAGE)
public class RTFGroupTable {
    private final boolean presence[];
    private final Map<MetricName, SegmentReader<RTFMetricTable>> metrics;

    public RTFGroupTable(group_table input, DictionaryDelta dictionary, SegmentReader.Factory<XdrAble> segmentFactory) {
        presence = FromXdr.bitset(input.presence);
        metrics = unmodifiableMap(metricsMap(input.metric_tbl, dictionary, segmentFactory));
    }

    private static Map<MetricName, SegmentReader<RTFMetricTable>> metricsMap(tables_metric tmArray[], DictionaryDelta dictionary, SegmentReader.Factory<XdrAble> segmentFactory) {
        return Arrays.stream(tmArray)
                .collect(Collectors.toMap(
                        tm -> MetricName.valueOf(dictionary.getPath(tm.metric_ref)),
                        tm -> segmentFactory.get(metric_table::new, new FilePos(tm.pos))
                                .map(mt -> new RTFMetricTable(mt, dictionary))
                                .peek(RTFMetricTable::validate)
                                .share()));
    }

    public void validate() {}

    public boolean contains(int index) {
        return index >= 0 && index < presence.length && presence[index];
    }

    public Set<MetricName> getMetricNames() {
        return metrics.keySet();
    }

    public SegmentReader<RTFMetricTable> getMetric(MetricName name) {
        return metrics.get(name);
    }
}
