/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.v0.xdr;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.xdr.support.ImmutableTimeSeriesValue;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.SimpleTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author ariane
 */
public class FromXdr {
    private FromXdr() {
    }

    public static SimpleGroupPath groupname(path p) {
        return SimpleGroupPath.valueOf(Arrays.stream(p.elems).map((pe) -> pe.value).collect(Collectors.toList()));
    }

    public static DateTime timestamp(timestamp_msec ts) {
        return new DateTime(ts.value, DateTimeZone.UTC);
    }

    public static MetricValue metricValue(metric_value mv) {
        switch (mv.kind) {
            default:
            case metrickind.EMPTY:
                return MetricValue.EMPTY;  // XXX should this throw?
            case metrickind.BOOL:
                return MetricValue.fromBoolean(mv.bool_value);
            case metrickind.INT:
                return MetricValue.fromIntValue(mv.int_value);
            case metrickind.FLOAT:
                return MetricValue.fromDblValue(mv.dbl_value);
            case metrickind.STRING:
                return MetricValue.fromStrValue(mv.str_value);
            case metrickind.HISTOGRAM:
                return MetricValue.fromHistValue(new Histogram(
                        Arrays.stream(mv.hist_value)
                        .map(he -> new Histogram.RangeWithCount(he.floor, he.ceil, he.events))));
        }
    }

    public static Entry<MetricName, MetricValue> metric(tsfile_metric m) {
        final MetricName m_name = MetricName.valueOf(Arrays.stream(m.metric.elems).map(p -> p.value).collect(Collectors.toList()));
        return SimpleMapEntry.create(m_name, metricValue(m.value));
    }

    public static Tags tags(tags t) {
        return Tags.valueOf(Arrays.stream(t.data)
                .collect(Collectors.toMap(tag -> tag.key, tag -> metricValue(tag.value))));
    }

    private static TimeSeriesValue time_series_value_(GroupName name, tsfile_metric m[]) {
        return new ImmutableTimeSeriesValue(name, Arrays.stream(m).map(FromXdr::metric), Map.Entry::getKey, Map.Entry::getValue);
    }

    private static Stream<TimeSeriesValue> process_tags_(SimpleGroupPath name, tsfile_tagged_datapoint dp[]) {
        return Arrays.stream(dp)
                .map(dp_elem -> {
                    final Tags tags = FromXdr.tags(dp_elem.tags);
                    final GroupName group = GroupName.valueOf(name, tags);
                    return time_series_value_(group, dp_elem.tsv);
                });
    }

    private static Stream<TimeSeriesValue> process_path_(tsfile_pathgroup pg[]) {
        return Arrays.stream(pg)
                .flatMap(pg_elem -> {
                    final SimpleGroupPath name = FromXdr.groupname(pg_elem.group);
                    return process_tags_(name, pg_elem.dps);
                });
    }

    public static TimeSeriesCollection datapoints(tsfile_datapoint tsv) {
        final DateTime ts = FromXdr.timestamp(tsv.ts);
        return new SimpleTimeSeriesCollection(ts, process_path_(tsv.groups));
    }
}
