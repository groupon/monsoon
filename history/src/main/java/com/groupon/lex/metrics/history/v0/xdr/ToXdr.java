package com.groupon.lex.metrics.history.v0.xdr;

import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author ariane
 */
public class ToXdr {
    private ToXdr() {}

    public static path groupname(SimpleGroupPath p) {
        path result = new path();
        result.elems = p.getPath().stream()
                .map((str) -> {
                    path_elem pe = new path_elem();
                    pe.value = str;
                    return pe;
                })
                .toArray(path_elem[]::new);
        return result;
    }

    public static path metricname(MetricName p) {
        path result = new path();
        result.elems = p.getPath().stream()
                .map((str) -> {
                    path_elem pe = new path_elem();
                    pe.value = str;
                    return pe;
                })
                .toArray(path_elem[]::new);
        return result;
    }

    public static timestamp_msec timestamp(DateTime ts) {
        timestamp_msec result = new timestamp_msec();
        result.value = ts.toDateTime(DateTimeZone.UTC).getMillis();
        return result;
    }

    public static metric_value metricValue(MetricValue mv) {
        metric_value result = new metric_value();

        result.bool_value = Optional.ofNullable(mv.getBoolValue()).orElse(Boolean.FALSE);
        result.int_value = Optional.ofNullable(mv.getIntValue()).orElse(0L);
        result.dbl_value = Optional.ofNullable(mv.getFltValue()).orElse(0D);
        result.str_value = Optional.ofNullable(mv.getStrValue()).orElse("");
        result.hist_value = Optional.ofNullable(mv.getHistValue())
                .map(Histogram::stream)
                .orElseGet(Stream::empty)
                .map(entry -> {
                    final histogram_entry he = new histogram_entry();
                    he.floor = entry.getRange().getFloor();
                    he.ceil = entry.getRange().getCeil();
                    he.events = entry.getCount();
                    return he;
                })
                .toArray(histogram_entry[]::new);

        result.kind = metrickind.EMPTY;  // Fallback.
        if (mv.getBoolValue() != null) result.kind = metrickind.BOOL;
        if (mv.getIntValue() != null) result.kind = metrickind.INT;
        if (mv.getFltValue() != null) result.kind = metrickind.FLOAT;
        if (mv.getStrValue() != null) result.kind = metrickind.STRING;
        if (mv.getHistValue() != null) result.kind = metrickind.HISTOGRAM;
        return result;
    }

    public static tsfile_metric metric(MetricName metric, MetricValue mv) {
        tsfile_metric result = new tsfile_metric();
        result.metric = metricname(metric);
        result.value = metricValue(mv);
        return result;
    }

    public static tags tags(Tags t) {
        tags result = new tags();
        result.data = t.stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map(tag_entry -> {
                    tag_elem r = new tag_elem();
                    r.key = tag_entry.getKey();
                    r.value = metricValue(tag_entry.getValue());
                    return r;
                })
                .toArray(tag_elem[]::new);
        return result;
    }

    private static tsfile_pathgroup datapoint(SimpleGroupPath path, Stream<TimeSeriesValue> tsv) {
        tsfile_pathgroup result = new tsfile_pathgroup();
        result.group = groupname(path);
        result.dps = tsv
                .map(v -> {
                    tsfile_tagged_datapoint tdp = new tsfile_tagged_datapoint();
                    tdp.tags = tags(v.getGroup().getTags());
                    tdp.tsv = v.getMetrics().entrySet().stream()
                            .filter(entry -> entry.getValue().isPresent())
                            .sorted(Comparator.comparing(Map.Entry::getKey))
                            .map(entry -> metric(entry.getKey(), entry.getValue()))
                            .toArray(tsfile_metric[]::new);
                    return tdp;
                })
                .toArray(tsfile_tagged_datapoint[]::new);
        return result;
    }

    public static tsfile_datapoint datapoints(TimeSeriesCollection tsdata) {
        tsfile_datapoint result = new tsfile_datapoint();
        result.ts = timestamp(tsdata.getTimestamp());
        result.groups = tsdata.getGroupPaths().stream()
                .map(path -> datapoint(path, tsdata.getTSValue(path).stream()))
                .toArray(tsfile_pathgroup[]::new);
        return result;
    }
}
