package com.groupon.lex.metrics.history.v1.xdr;

import com.google.common.collect.BiMap;
import static com.groupon.lex.metrics.ConfigSupport.quotedString;
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author ariane
 */
public class ToXdr {
    private static final Logger LOG = Logger.getLogger(ToXdr.class.getName());
    private final FromXdr from_;

    public ToXdr(FromXdr from) { from_ = requireNonNull(from); }

    public static timestamp_msec timestamp(DateTime ts) {
        timestamp_msec result = new timestamp_msec();
        result.value = ts.toDateTime(DateTimeZone.UTC).getMillis();
        return result;
    }

    private metric_value metric_value_(dictionary_delta dict_delta, MetricValue mv) {
        metric_value result = new metric_value();
        result.kind = metrickind.EMPTY;  // Fallback.

        if (mv.getBoolValue() != null) {
            result.kind = metrickind.BOOL;
            result.bool_value = mv.getBoolValue();
        }
        if (mv.getIntValue() != null) {
            result.kind = metrickind.INT;
            result.int_value = mv.getIntValue();
        }
        if (mv.getFltValue() != null) {
            result.kind = metrickind.FLOAT;
            result.dbl_value = mv.getFltValue();
        }
        if (mv.getStrValue() != null) {
            result.kind = metrickind.STRING;
            result.str_dict_ref = string_index_(dict_delta, mv);
        }
        if (mv.getHistValue() != null) {
            result.kind = metrickind.HISTOGRAM;
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
        }

        return result;
    }

    /** Transform a TimeSeriesCollection into the XDR serialized form. */
    public tsfile_data data(TimeSeriesCollection ts_data) {
        final dictionary_delta dict_delta = new dictionary_delta();
        dict_delta.gdd = new path_dictionary_delta[0];
        dict_delta.mdd = new path_dictionary_delta[0];
        dict_delta.sdd = new strval_dictionary_delta[0];
        dict_delta.tdd = new tag_dictionary_delta[0];

        final List<tsfile_record> records = new ArrayList<>();
        final Iterator<TimeSeriesValue> tsv_iter = ts_data.getTSValues().stream().iterator();
        while (tsv_iter.hasNext()) {
            final TimeSeriesValue tsv = tsv_iter.next();
            tsfile_record record = new tsfile_record();

            record.group_ref = simpleGroupPath_index(dict_delta, tsv.getGroup().getPath());
            record.tag_ref = tags_index(dict_delta, tsv.getGroup().getTags());
            record.metrics = tsv.getMetrics().entrySet().stream()
                    .map(entry -> {
                        tsfile_record_entry tre = new tsfile_record_entry();
                        tre.metric_ref = metricName_index(dict_delta, entry.getKey());
                        tre.v = metric_value_(dict_delta, entry.getValue());
                        return tre;
                    })
                    .toArray(tsfile_record_entry[]::new);
            records.add(record);
        }

        tsfile_data result = new tsfile_data();
        result.records = records.stream().toArray(tsfile_record[]::new);
        result.ts = timestamp(ts_data.getTimestamp());
        if (dict_delta.gdd.length > 0 || dict_delta.mdd.length > 0 || dict_delta.sdd.length > 0 || dict_delta.tdd.length > 0)
            result.dd = dict_delta;
        return result;
    }

    /** Lookup the index for the argument, or if it isn't present, create one. */
    private int simpleGroupPath_index(dictionary_delta dict_delta, SimpleGroupPath group) {
        final BiMap<SimpleGroupPath, Integer> dict = from_.getGroupDict().inverse();
        final Integer resolved = dict.get(group);
        if (resolved != null) return resolved;

        final int allocated = allocate_index_(dict);
        dict.put(group, allocated);

        // Create new pdd for serialization.
        path_dictionary_delta gdd = new path_dictionary_delta();
        gdd.id = allocated;
        gdd.value = new_path_(group.getPath());
        // Append new entry to array.
        dict_delta.gdd = Stream.concat(Arrays.stream(dict_delta.gdd), Stream.of(gdd))
                .toArray(path_dictionary_delta[]::new);
        LOG.log(Level.FINE, "dict_delta.gdd: {0} items (added {1})", new Object[]{dict_delta.gdd.length, group});

        return allocated;
    }

    /** Lookup the index for the argument, or if it isn't present, create one. */
    private int metricName_index(dictionary_delta dict_delta, MetricName metric) {
        final BiMap<MetricName, Integer> dict = from_.getMetricDict().inverse();
        final Integer resolved = dict.get(metric);
        if (resolved != null) return resolved;

        final int allocated = allocate_index_(dict);
        dict.put(metric, allocated);

        // Create new pdd for serialization.
        path_dictionary_delta mdd = new path_dictionary_delta();
        mdd.id = allocated;
        mdd.value = new_path_(metric.getPath());
        // Append new entry to array.
        dict_delta.mdd = Stream.concat(Arrays.stream(dict_delta.mdd), Stream.of(mdd))
                .toArray(path_dictionary_delta[]::new);
        LOG.log(Level.FINE, "dict_delta.mdd: {0} items (added {1})", new Object[]{dict_delta.mdd.length, metric});

        return allocated;
    }

    /** Lookup the index for the argument, or if it isn't present, create one. */
    private int tags_index(dictionary_delta dict_delta, Tags tags) {
        final BiMap<Tags, Integer> dict = from_.getTagDict().inverse();
        final Integer resolved = dict.get(tags);
        if (resolved != null) return resolved;

        final int allocated = allocate_index_(dict);
        dict.put(tags, allocated);

        // Create new pdd for serialization.
        tag_dictionary_delta tdd = new tag_dictionary_delta();
        tdd.id = allocated;
        tdd.value = new tags();
        tdd.value.elems = tags.stream()
                .map(entry -> {
                    tag_elem elem = new tag_elem();
                    elem.key = entry.getKey();
                    elem.value = metric_value_(dict_delta, entry.getValue());
                    return elem;
                })
                .toArray(tag_elem[]::new);
        // Append new entry to array.
        dict_delta.tdd = Stream.concat(Arrays.stream(dict_delta.tdd), Stream.of(tdd))
                .toArray(tag_dictionary_delta[]::new);
        LOG.log(Level.FINE, "dict_delta.tdd: {0} items (added {1})", new Object[]{dict_delta.tdd.length, tags});

        return allocated;
    }

    /** Lookup the index for the argument, or if it isn't present, create one. */
    private int string_index_(dictionary_delta dict_delta, MetricValue str) {
        assert(str.getStrValue() != null);
        final BiMap<MetricValue, Integer> dict = from_.getStrvalDict().inverse();
        final Integer resolved = dict.get(str);
        if (resolved != null) return resolved;

        final int allocated = allocate_index_(dict);
        dict.put(str, allocated);

        // Create new pdd for serialization.
        strval_dictionary_delta sdd = new strval_dictionary_delta();
        sdd.id = allocated;
        sdd.value = str.getStrValue();
        // Append new entry to array.
        dict_delta.sdd = Stream.concat(Arrays.stream(dict_delta.sdd), Stream.of(sdd))
                .toArray(strval_dictionary_delta[]::new);
        LOG.log(Level.FINE, "dict_delta.sdd: {0} items (added {1})", new Object[]{dict_delta.sdd.length, quotedString(sdd.value)});

        return allocated;
    }

    private static int allocate_index_(BiMap<?, Integer> dict) {
        return dict.values().stream()
                .mapToInt(i -> i + 1)
                .max()
                .orElse(0);
    }

    private static path new_path_(List<String> strings) {
        final path p = new path();
        p.elems = strings.stream()
                .map(s -> {
                    path_elem pe = new path_elem();
                    pe.value = s;
                    return pe;
                })
                .toArray(path_elem[]::new);
        return p;
    }
}
