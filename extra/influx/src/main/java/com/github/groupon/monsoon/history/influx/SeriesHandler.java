/*
 * Copyright (c) 2017, ariane
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.groupon.monsoon.history.influx;

import static com.github.groupon.monsoon.history.influx.InfluxUtil.TIME_COLUMN;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.ImmutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.SimpleTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import gnu.trove.map.hash.THashMap;
import java.time.Instant;
import java.util.Collection;
import static java.util.Collections.unmodifiableSet;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.influxdb.dto.QueryResult;
import org.joda.time.DateTime;

/**
 * Handles an ordered list of series and exposes them as TimeSeriesCollections.
 */
public class SeriesHandler {
    private final Multimap<DateTime, IntermediateTSV> datums = MultimapBuilder
            .treeKeys()
            .arrayListValues()
            .build();

    public Set<DateTime> keySet() {
        return unmodifiableSet(datums.keySet());
    }

    public Stream<TimeSeriesCollection> build() {
        return datums.asMap().entrySet().stream()
                .map(timestampedTsc -> buildTsc(timestampedTsc.getKey(), timestampedTsc.getValue()));
    }

    public void addSeries(QueryResult.Series series) {
        final GroupName group = seriesToGroupName(series);
        final int timeColumnIdx = InfluxUtil.getColumnIndexFromSeries(series, TIME_COLUMN).orElseThrow(() -> new IllegalStateException("missing time column"));

        series.getValues().forEach(row -> {
            assert series.getColumns().size() == row.size();

            final DateTime timestamp = new DateTime((Instant) row.get(timeColumnIdx));
            final IntermediateTSV valueMap = new IntermediateTSV(group);

            final ListIterator<String> columnIter = series.getColumns().listIterator();
            final Iterator<Object> rowIter = row.iterator();
            while (rowIter.hasNext()) {
                final int columnIdx = columnIter.nextIndex();
                final String columnName = columnIter.next();
                if (columnIdx != timeColumnIdx)
                    valueMap.getMetrics().put(valueKeyToMetricName(columnName), seriesValueToMetricValue(rowIter.next()));
            }

            datums.put(timestamp, valueMap);
        });
    }

    public void merge(SeriesHandler other) {
        datums.putAll(other.datums);
    }

    private static TimeSeriesCollection buildTsc(DateTime timestamp, Collection<IntermediateTSV> c) {
        return new SimpleTimeSeriesCollection(timestamp, mergedTimeseriesValues(c));
    }

    private static Stream<TimeSeriesValue> mergedTimeseriesValues(Collection<IntermediateTSV> c) {
        return c.stream()
                .collect(Collectors.groupingBy(
                        IntermediateTSV::getGroup,
                        Collectors.reducing((IntermediateTSV x, IntermediateTSV y) -> {
                            assert Objects.equals(x.getGroup(), y.getGroup());

                            x.getMetrics().putAll(y.getMetrics());
                            return x;
                        })))
                .values().stream()
                .map(Optional::get)
                .map(IntermediateTSV::build);
    }

    private static GroupName seriesToGroupName(QueryResult.Series series) {
        final SimpleGroupPath groupPath = pathStrToGroupPath(series.getName());
        final Tags tags = Tags.valueOf(series.getTags().entrySet().stream()
                .filter(tagEntry -> tagEntry.getValue() != null)
                .map(tagEntry -> SimpleMapEntry.create(tagEntry.getKey(), tagValueToMetricValue(tagEntry.getValue()))));
        return GroupName.valueOf(groupPath, tags);
    }

    private static SimpleGroupPath pathStrToGroupPath(String str) {
        return SimpleGroupPath.valueOf(str.split(Pattern.quote(".")));
    }

    private static MetricName valueKeyToMetricName(String str) {
        return MetricName.valueOf(str.split(Pattern.quote(".")));
    }

    private static MetricValue seriesValueToMetricValue(Object obj) {
        if (obj instanceof Boolean)
            return MetricValue.fromBoolean((Boolean) obj);
        if (obj instanceof Number)
            return MetricValue.fromNumberValue((Number) obj);
        assert obj instanceof String;
        return MetricValue.fromStrValue(obj.toString());
    }

    private static MetricValue tagValueToMetricValue(String str) {
        if ("true".equals(str))
            return MetricValue.TRUE;
        if ("false".equals(str))
            return MetricValue.FALSE;

        try {
            return MetricValue.fromIntValue(Long.parseLong(str));
        } catch (NumberFormatException ex) {
            /* SKIP: value is not an integer */
        }

        try {
            return MetricValue.fromDblValue(Double.parseDouble(str));
        } catch (NumberFormatException ex) {
            /* SKIP: value is not a floating point value */
        }

        return MetricValue.fromStrValue(str);
    }

    @RequiredArgsConstructor
    @Getter
    private static class IntermediateTSV {
        private final GroupName group;
        private final Map<MetricName, MetricValue> metrics = new THashMap<>();

        public TimeSeriesValue build() {
            return new ImmutableTimeSeriesValue(group, metrics);
        }
    }
}
