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
import static java.util.Collections.singletonMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author ariane
 */
public interface TimeSeriesCollection extends Cloneable {
    public static DateTime now() {
        return DateTime.now(DateTimeZone.UTC);
    }

    public TimeSeriesCollection add(TimeSeriesValue tsv);
    public TimeSeriesCollection renameGroup(GroupName oldname, GroupName newname);

    public default TimeSeriesCollection addMetric(GroupName group, MetricName metric, MetricValue value) {
        return addMetrics(group, singletonMap(metric, value));
    }

    public TimeSeriesCollection addMetrics(GroupName group, Map<MetricName, MetricValue> metrics);

    public DateTime getTimestamp();
    public boolean isEmpty();

    public static MutableTimeSeriesCollection empty(DateTime timestamp) {
        return new MutableTimeSeriesCollection(timestamp, Stream.empty());
    }

    public static MutableTimeSeriesCollection empty() {
        return empty(now());
    }

    public Set<GroupName> getGroups();
    public Set<SimpleGroupPath> getGroupPaths();
    public TimeSeriesValueSet getTSValues();
    public TimeSeriesValueSet getTSValue(SimpleGroupPath name);
    public Optional<TimeSeriesValue> get(GroupName name);

    public default Optional<TimeSeriesValueSet> getTSDeltaByName(GroupName name) {
        return get(name)
                .map(Stream::of)
                .map(TimeSeriesValueSet::new);
    }

    public TimeSeriesCollection clone();
}
