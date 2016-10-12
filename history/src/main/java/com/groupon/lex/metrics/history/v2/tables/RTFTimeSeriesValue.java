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

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.history.xdr.support.reader.SegmentReader;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.AbstractTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author ariane
 */
public class RTFTimeSeriesValue extends AbstractTimeSeriesValue {
    private final long ts;
    private final int index;
    @Getter
    private final GroupName group;
    private final RTFGroupTable tbl;

    public RTFTimeSeriesValue(long ts, int index, GroupName group, RTFGroupTable tbl) {
        this.ts = ts;
        this.index = index;
        this.group = group;
        this.tbl = tbl;
    }

    @Override
    public DateTime getTimestamp() {
        return new DateTime(ts, DateTimeZone.UTC);
    }

    @Override
    public Map<MetricName, MetricValue> getMetrics() {
        return tbl.getMetrics().entrySet().stream()
                .map(entry -> SimpleMapEntry.create(entry.getKey(), entry.getValue().decodeOrThrow()))
                .filter(entry -> entry.getValue().contains(index))
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get(index)));
    }

    @Override
    public Optional<MetricValue> findMetric(MetricName name) {
        return Optional.ofNullable(tbl.getMetrics().get(name))
                .map(SegmentReader::decodeOrThrow)
                .filter(mvTbl -> mvTbl.contains(index))
                .map(mvTbl -> mvTbl.get(index));
    }

    @Override
    public TimeSeriesValue clone() {
        return this;  // Read-only value needs no copy.
    }
}
