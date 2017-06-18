package com.groupon.lex.metrics.history.v1.xdr;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import static com.groupon.lex.metrics.ConfigSupport.quotedString;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.ImmutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.SimpleTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author ariane
 */
public class FromXdr {
    private static final Logger LOG = Logger.getLogger(FromXdr.class.getName());
    private final BiMap<Integer, SimpleGroupPath> group_dict = HashBiMap.create();
    private final BiMap<Integer, Tags> tag_dict = HashBiMap.create();
    private final BiMap<Integer, MetricName> metric_dict = HashBiMap.create();
    private final BiMap<Integer, MetricValue> strval_dict = HashBiMap.create();

    public FromXdr() {
    }

    public BiMap<Integer, SimpleGroupPath> getGroupDict() {
        return group_dict;
    }

    public BiMap<Integer, Tags> getTagDict() {
        return tag_dict;
    }

    public BiMap<Integer, MetricName> getMetricDict() {
        return metric_dict;
    }

    public BiMap<Integer, MetricValue> getStrvalDict() {
        return strval_dict;
    }

    public static DateTime timestamp(timestamp_msec ts) {
        return new DateTime(ts.value, DateTimeZone.UTC);
    }

    private MetricValue metric_value_(metric_value mv) {
        switch (mv.kind) {
            default:
            case metrickind.EMPTY:
                return MetricValue.EMPTY;
            case metrickind.BOOL:
                return MetricValue.fromBoolean(mv.bool_value);
            case metrickind.INT:
                return MetricValue.fromIntValue(mv.int_value);
            case metrickind.FLOAT:
                return MetricValue.fromDblValue(mv.dbl_value);
            case metrickind.STRING:
                return Optional.ofNullable(strval_dict.get(mv.str_dict_ref)).orElseThrow(() -> new RuntimeException("broken string reference " + mv.str_dict_ref));
            case metrickind.HISTOGRAM:
                return MetricValue.fromHistValue(new Histogram(
                        Arrays.stream(mv.hist_value)
                        .map(he -> new Histogram.RangeWithCount(he.floor, he.ceil, he.events))));
        }
    }

    private MetricName metric_name_(path p) {
        return MetricName.valueOf(Arrays.stream(p.elems)
                .map(elem -> elem.value)
                .collect(Collectors.toList()));
    }

    private SimpleGroupPath group_name_(path p) {
        return SimpleGroupPath.valueOf(Arrays.stream(p.elems)
                .map(elem -> elem.value)
                .collect(Collectors.toList()));
    }

    private Tags tags_(tags t) {
        return Tags.valueOf(Arrays.stream(t.elems)
                .map(elem -> SimpleMapEntry.create(elem.key, metric_value_(elem.value))));
    }

    private TimeSeriesValue decode_record_(DateTime ts, tsfile_record record) {
        final SimpleGroupPath group_name = Optional.ofNullable(group_dict.get(record.group_ref)).orElseThrow(() -> new RuntimeException("broken tag reference " + record.group_ref));
        final Tags tags = Optional.ofNullable(tag_dict.get(record.tag_ref)).orElseThrow(() -> new RuntimeException("broken tag reference: " + record.tag_ref));
        final Stream<Map.Entry<MetricName, MetricValue>> metrics = Arrays.stream(record.metrics)
                .map((tsfile_record_entry metric) -> {
                    final MetricName mname = Optional.ofNullable(metric_dict.get(metric.metric_ref)).orElseThrow(() -> new RuntimeException("broken path reference " + metric.metric_ref));
                    final MetricValue mvalue = metric_value_(metric.v);
                    return SimpleMapEntry.create(mname, mvalue);
                });
        return new ImmutableTimeSeriesValue(GroupName.valueOf(group_name, tags), metrics, Map.Entry::getKey, Map.Entry::getValue);
    }

    private void update_dict_(dictionary_delta dd) {
        for (strval_dictionary_delta sdd : dd.sdd) {
            // Load strings before tags, since tags use MetricValues, which end up refering to the strings.
            LOG.log(Level.FINE, "string {0} -> {1}", new Object[]{sdd.id, quotedString(sdd.value)});
            strval_dict.put(sdd.id, MetricValue.fromStrValue(sdd.value));
        }
        for (path_dictionary_delta gdd : dd.gdd) {
            final SimpleGroupPath value = group_name_(gdd.value);
            LOG.log(Level.FINE, "group name {0} -> {1}", new Object[]{gdd.id, value});
            group_dict.put(gdd.id, value);
        }
        for (path_dictionary_delta mdd : dd.mdd) {
            final MetricName value = metric_name_(mdd.value);
            LOG.log(Level.FINE, "metric name {0} -> {1}", new Object[]{mdd.id, value});
            metric_dict.put(mdd.id, value);
        }
        for (tag_dictionary_delta tdd : dd.tdd) {
            final Tags value = tags_(tdd.value);
            LOG.log(Level.FINE, "tags {0} -> {1}", new Object[]{tdd.id, value});
            tag_dict.put(tdd.id, value);
        }
    }

    public TimeSeriesCollection data(tsfile_data data) {
        final DateTime ts = timestamp(data.ts);
        Optional.ofNullable(data.dd).ifPresent(this::update_dict_);
        return new SimpleTimeSeriesCollection(
                ts,
                Arrays.stream(data.records).map(record -> decode_record_(ts, record)));
    }
}
