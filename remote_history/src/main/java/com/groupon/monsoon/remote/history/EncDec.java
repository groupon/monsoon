/*
 * Copyright (c) 2016, Ariane van der Steldt
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
package com.groupon.monsoon.remote.history;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.ImmutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.SimpleTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.monsoon.remote.history.xdr.dictionary_delta;
import com.groupon.monsoon.remote.history.xdr.duration_msec;
import com.groupon.monsoon.remote.history.xdr.evaluate_iter_response;
import com.groupon.monsoon.remote.history.xdr.evaluate_iter_response_success;
import com.groupon.monsoon.remote.history.xdr.evaluate_response;
import com.groupon.monsoon.remote.history.xdr.histogram_entry;
import com.groupon.monsoon.remote.history.xdr.iter_result_code;
import com.groupon.monsoon.remote.history.xdr.list_of_timeseries_collection;
import com.groupon.monsoon.remote.history.xdr.metric_value;
import com.groupon.monsoon.remote.history.xdr.metrickind;
import com.groupon.monsoon.remote.history.xdr.named_evaluation;
import com.groupon.monsoon.remote.history.xdr.named_evaluation_map;
import com.groupon.monsoon.remote.history.xdr.named_evaluation_map_entry;
import com.groupon.monsoon.remote.history.xdr.named_evaluation_set;
import com.groupon.monsoon.remote.history.xdr.path;
import com.groupon.monsoon.remote.history.xdr.path_dictionary_delta;
import com.groupon.monsoon.remote.history.xdr.path_elem;
import com.groupon.monsoon.remote.history.xdr.stream_iter_tsc_response;
import com.groupon.monsoon.remote.history.xdr.stream_iter_tsc_response_success;
import com.groupon.monsoon.remote.history.xdr.stream_response;
import com.groupon.monsoon.remote.history.xdr.strval_dictionary_delta;
import com.groupon.monsoon.remote.history.xdr.tag_dictionary_delta;
import com.groupon.monsoon.remote.history.xdr.tag_elem;
import com.groupon.monsoon.remote.history.xdr.tagged_metric_value;
import com.groupon.monsoon.remote.history.xdr.tags;
import com.groupon.monsoon.remote.history.xdr.timeseries_collection;
import com.groupon.monsoon.remote.history.xdr.timeseries_metric_delta_set;
import com.groupon.monsoon.remote.history.xdr.timestamp_msec;
import com.groupon.monsoon.remote.history.xdr.tsfile_record;
import com.groupon.monsoon.remote.history.xdr.tsfile_record_entry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

public class EncDec {
    private static final Logger LOG = Logger.getLogger(EncDec.class.getName());

    public static interface IterSuccessResponse<T> {
        public List<T> getData();

        public boolean isLast();

        public long getCookie();
    }

    @Value
    public static class IterSuccessResponseImpl<T> implements IterSuccessResponse<T> {
        private final List<T> data;
        private final boolean last;
        private final long cookie;
    }

    @Value
    public static class NewIterResponse<T> implements IterSuccessResponse<T> {
        private final long iterIdx;
        private final List<T> data;
        private final boolean last;
        private final long cookie;
    }

    @Value
    public static class IterErrorResponse {
        private final IteratorErrorCode error;
    }

    public static stream_iter_tsc_response encodeStreamIterTscResponse(IterErrorResponse r) {
        stream_iter_tsc_response result = new stream_iter_tsc_response();
        result.result = r.getError().getEncoded();
        return result;
    }

    public static stream_iter_tsc_response encodeStreamIterTscResponse(IterSuccessResponse<TimeSeriesCollection> r) {
        stream_iter_tsc_response result = new stream_iter_tsc_response();
        result.result = iter_result_code.SUCCESS;
        result.response = new stream_iter_tsc_response_success();
        result.response.last = r.isLast();
        result.response.rv = encodeTSCCollection(r.getData());
        result.response.cookie = r.getCookie();
        return result;
    }

    public static Any2<IterSuccessResponse<TimeSeriesCollection>, IterErrorResponse> decodeStreamIterTscResponse(stream_iter_tsc_response r) {
        final Any2<IterSuccessResponse<TimeSeriesCollection>, IterErrorResponse> result;
        switch (r.result) {
            case iter_result_code.SUCCESS:
                result = Any2.left(new IterSuccessResponseImpl<>(decodeTSCCollection(r.response.rv), r.response.last, r.response.cookie));
                break;
            default:
                result = Any2.right(new IterErrorResponse(IteratorErrorCode.fromEncodedForm(r.result)));
        }
        return result;
    }

    public static stream_response encodeStreamResponse(NewIterResponse<? extends TimeSeriesCollection> sr) {
        stream_response result = new stream_response();
        result.iter_id = sr.getIterIdx();
        result.first_response = new stream_iter_tsc_response_success();
        result.first_response.last = sr.isLast();
        result.first_response.rv = encodeTSCCollection(sr.getData());
        result.first_response.cookie = sr.getCookie();
        return result;
    }

    public static NewIterResponse<TimeSeriesCollection> decodeStreamResponse(stream_response sr) {
        return new NewIterResponse<>(sr.iter_id, decodeTSCCollection(sr.first_response.rv), sr.first_response.last, sr.first_response.cookie);
    }

    public static list_of_timeseries_collection encodeTSCCollection(Collection<? extends TimeSeriesCollection> c) {
        final ActiveDict dict = new ActiveDict();
        final list_of_timeseries_collection lotc = new list_of_timeseries_collection();
        lotc.collections = c.stream()
                .map(tsc -> encodeTSC(dict, tsc))
                .toArray(timeseries_collection[]::new);
        return lotc;
    }

    public static List<TimeSeriesCollection> decodeTSCCollection(list_of_timeseries_collection c) {
        final ActiveDict dict = new ActiveDict();
        return Arrays.stream(c.collections)
                .map(tsc -> decodeTSC(dict, tsc))
                .collect(Collectors.toList());
    }

    private static timeseries_collection encodeTSC(ActiveDict dict, TimeSeriesCollection tsc) {
        final DictDelta dd = new DictDelta(dict);

        timeseries_collection result = new timeseries_collection();
        result.ts = encodeTimestamp(tsc.getTimestamp());
        result.records = tsc.getTSValues().stream()
                .map(tsv -> encodeTSV(dd, tsv))
                .toArray(tsfile_record[]::new);
        result.dd = dd.encodedForm();
        return result;
    }

    private static TimeSeriesCollection decodeTSC(ActiveDict dict, timeseries_collection tsc) {
        final DateTime ts = decodeTimestamp(tsc.ts);
        if (tsc.dd != null) dict.apply(tsc.dd);
        return new SimpleTimeSeriesCollection(ts, Arrays.stream(tsc.records).map(tsv -> decodeTSV(dict, ts, tsv)));
    }

    public static timeseries_collection encodeTSC(TimeSeriesCollection tsc) {
        return encodeTSC(new ActiveDict(), tsc);
    }

    public static TimeSeriesCollection decodeTSC(timeseries_collection tsc) {
        return decodeTSC(new ActiveDict(), tsc);
    }

    private static tsfile_record encodeTSV(DictDelta dd, TimeSeriesValue tsv) {
        tsfile_record record = new tsfile_record();
        record.group_ref = dd.getGroup(tsv.getGroup().getPath());
        record.tag_ref = dd.getTags(tsv.getGroup().getTags());
        record.metrics = tsv.getMetrics().entrySet().stream()
                .map(m -> {
                    tsfile_record_entry tre = new tsfile_record_entry();
                    tre.metric_ref = dd.getMetricName(m.getKey());
                    tre.v = encodeMetricValue(dd, m.getValue());
                    return tre;
                })
                .toArray(tsfile_record_entry[]::new);
        return record;
    }

    private static TimeSeriesValue decodeTSV(ActiveDict dict, DateTime ts, tsfile_record tsv) {
        return new ImmutableTimeSeriesValue(
                GroupName.valueOf(dict.getGroup(tsv.group_ref), dict.getTags(tsv.tag_ref)),
                Arrays.stream(tsv.metrics),
                m -> dict.getMetricName(m.metric_ref),
                m -> decodeMetricValue(dict, m.v));
    }

    private static metric_value encodeMetricValue(DictDelta dd, MetricValue mv) {
        metric_value result = new metric_value();
        result.kind = metrickind.EMPTY;
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
            result.str_dict_ref = dd.getString(mv.getStrValue());
        }
        if (mv.getHistValue() != null) {
            result.kind = metrickind.HISTOGRAM;
            result.hist_value = encodeHistValue(mv.getHistValue());
        }
        return result;
    }

    private static MetricValue decodeMetricValue(ActiveDict dd, metric_value mv) {
        final MetricValue result;
        switch (mv.kind) {
            case metrickind.BOOL:
                result = MetricValue.fromBoolean(mv.bool_value);
                break;
            case metrickind.INT:
                result = MetricValue.fromIntValue(mv.int_value);
                break;
            case metrickind.FLOAT:
                result = MetricValue.fromDblValue(mv.dbl_value);
                break;
            case metrickind.STRING:
                result = MetricValue.fromStrValue(dd.getString(mv.str_dict_ref));
                break;
            case metrickind.HISTOGRAM:
                result = MetricValue.fromHistValue(decodeHistValue(mv.hist_value));
                break;
            case metrickind.EMPTY:
                result = MetricValue.EMPTY;
                break;
            default:
                throw new IllegalArgumentException("metric kind " + mv.kind + " not recognized");
        }
        return result;
    }

    private static histogram_entry[] encodeHistValue(Histogram h) {
        return h.stream()
                .map(entry -> {
                    histogram_entry he = new histogram_entry();
                    he.floor = entry.getRange().getFloor();
                    he.ceil = entry.getRange().getCeil();
                    he.events = entry.getCount();
                    return he;
                })
                .toArray(histogram_entry[]::new);
    }

    private static Histogram decodeHistValue(histogram_entry h[]) {
        return new Histogram(Arrays.stream(h)
                .map(he -> new Histogram.RangeWithCount(he.floor, he.ceil, he.events)));
    }

    public static timestamp_msec encodeTimestamp(DateTime ts) {
        return new timestamp_msec(ts.toDateTime(DateTimeZone.UTC).getMillis());
    }

    public static DateTime decodeTimestamp(timestamp_msec ts) {
        return new DateTime(ts.value, DateTimeZone.UTC);
    }

    public static duration_msec encodeDuration(Duration d) {
        return new duration_msec(d.getMillis());
    }

    public static Duration decodeDuration(duration_msec d) {
        return new Duration(d.value);
    }

    public static named_evaluation_map encodeEvaluationMap(Map<String, ? extends TimeSeriesMetricExpression> expressions) {
        named_evaluation_map nem = new named_evaluation_map();
        nem.entries = expressions.entrySet().stream()
                .map(expr -> {
                    named_evaluation_map_entry eme = new named_evaluation_map_entry();
                    eme.name = expr.getKey();
                    eme.expr = expr.getValue().configString().toString();
                    return eme;
                })
                .toArray(named_evaluation_map_entry[]::new);
        return nem;
    }

    public static Map<String, ? extends TimeSeriesMetricExpression> decodeEvaluationMap(named_evaluation_map expressions) {
        return Arrays.stream(expressions.entries)
                .collect(Collectors.toMap(
                        eme -> eme.name,
                        eme -> {
                            try {
                                return TimeSeriesMetricExpression.valueOf(eme.expr);
                            } catch (TimeSeriesMetricExpression.ParseException ex) {
                                LOG.log(Level.WARNING, "parse failed for " + eme.expr, ex);
                                throw new RuntimeException("unable to parse expression", ex);
                            }
                        }));
    }

    public static evaluate_iter_response encodeEvaluateIterResponse(IterSuccessResponse<Collection<CollectHistory.NamedEvaluation>> r) {
        evaluate_iter_response result = new evaluate_iter_response();
        result.result = iter_result_code.SUCCESS;
        result.response = new evaluate_iter_response_success();
        result.response.last = r.isLast();
        result.response.rv = encodeNamedEvaluation(r.getData());
        result.response.cookie = r.getCookie();
        return result;
    }

    public static evaluate_iter_response encodeEvaluateIterResponse(IterErrorResponse r) {
        evaluate_iter_response result = new evaluate_iter_response();
        result.result = r.getError().getEncoded();
        return result;
    }

    public static Any2<IterSuccessResponse<Collection<CollectHistory.NamedEvaluation>>, IterErrorResponse> decodeEvaluateIterResponse(evaluate_iter_response r) {
        final Any2<IterSuccessResponse<Collection<CollectHistory.NamedEvaluation>>, IterErrorResponse> result;
        switch (r.result) {
            case iter_result_code.SUCCESS:
                result = Any2.left(new IterSuccessResponseImpl<Collection<CollectHistory.NamedEvaluation>>(decodeNamedEvaluation(r.response.rv), r.response.last, r.response.cookie));
                break;
            default:
                result = Any2.right(new IterErrorResponse(IteratorErrorCode.fromEncodedForm(r.result)));
        }
        return result;
    }

    public static evaluate_response encodeEvaluateResponse(NewIterResponse<Collection<CollectHistory.NamedEvaluation>> r) {
        evaluate_response result = new evaluate_response();
        result.iter_id = r.getIterIdx();
        result.first_response = new evaluate_iter_response_success();
        result.first_response.last = r.isLast();
        result.first_response.rv = encodeNamedEvaluation(r.getData());
        result.first_response.cookie = r.getCookie();
        return result;
    }

    public static NewIterResponse<Collection<CollectHistory.NamedEvaluation>> decodeEvaluateResponse(evaluate_response r) {
        return new NewIterResponse<>(r.iter_id, decodeNamedEvaluation(r.first_response.rv), r.first_response.last, r.first_response.cookie);
    }

    private static named_evaluation_set[] encodeNamedEvaluation(Collection<Collection<CollectHistory.NamedEvaluation>> nes) {
        final ActiveDict dict = new ActiveDict();
        return nes.stream()
                .map(ne_set -> {
                    final DictDelta dd = new DictDelta(dict);
                    named_evaluation_set set = new named_evaluation_set();
                    set.entries = ne_set.stream()
                            .map(ne -> {
                                named_evaluation result = new named_evaluation();
                                result.name = ne.getName();
                                result.ts = encodeTimestamp(ne.getDatetime());
                                result.ts_set = encodeTimeSeriesMetricDeltaSet(dd, ne.getTS());
                                return result;
                            })
                            .toArray(named_evaluation[]::new);
                    set.dd = dd.encodedForm();
                    return set;
                })
                .toArray(named_evaluation_set[]::new);
    }

    private static List<Collection<CollectHistory.NamedEvaluation>> decodeNamedEvaluation(named_evaluation_set nes[]) {
        final ActiveDict dict = new ActiveDict();
        return Arrays.stream(nes)
                .map(nes_entry -> {
                    if (nes_entry.dd != null) dict.apply(nes_entry.dd);
                    return Arrays.stream(nes_entry.entries)
                            .map(ne -> new CollectHistory.NamedEvaluation(ne.name, decodeTimestamp(ne.ts), decodeTimeSeriesMetricDeltaSet(dict, ne.ts_set)))
                            .collect(Collectors.toList());
                })
                .collect(Collectors.toList());
    }

    private static timeseries_metric_delta_set encodeTimeSeriesMetricDeltaSet(DictDelta dd, TimeSeriesMetricDeltaSet tmds) {
        timeseries_metric_delta_set result = new timeseries_metric_delta_set();
        result.is_vector = tmds.isVector();
        if (result.is_vector) {
            result.vector = tmds.asVector().orElseThrow(IllegalStateException::new).entrySet().stream()
                    .map(taggedValue -> {
                        tagged_metric_value tmv_result = new tagged_metric_value();
                        tmv_result.tag_ref = dd.getTags(taggedValue.getKey());
                        tmv_result.v = encodeMetricValue(dd, taggedValue.getValue());
                        return tmv_result;
                    })
                    .toArray(tagged_metric_value[]::new);
        } else {
            result.scalar = encodeMetricValue(dd, tmds.asScalar().orElseThrow(IllegalStateException::new));
        }
        return result;
    }

    private static TimeSeriesMetricDeltaSet decodeTimeSeriesMetricDeltaSet(ActiveDict dd, timeseries_metric_delta_set tmds) {
        if (tmds.is_vector) {
            return new TimeSeriesMetricDeltaSet(Arrays.stream(tmds.vector)
                    .map(taggedValue -> SimpleMapEntry.create(dd.getTags(taggedValue.tag_ref), decodeMetricValue(dd, taggedValue.v))));
        } else {
            return new TimeSeriesMetricDeltaSet(decodeMetricValue(dd, tmds.scalar));
        }
    }

    @RequiredArgsConstructor
    public static class DictDelta {
        private final ActiveDict dict;
        private final List<path_dictionary_delta> addedGroups = new ArrayList<>();
        private final List<tag_dictionary_delta> addedTags = new ArrayList<>();
        private final List<path_dictionary_delta> addedMetricNames = new ArrayList<>();
        private final List<strval_dictionary_delta> addedStrings = new ArrayList<>();

        public int getGroup(SimpleGroupPath group) {
            return dict.getOrCreateGroup(group, (idx, g) -> addedGroups.add(encodePathDictionaryDelta(idx, g)));
        }

        public int getTags(Tags tags) {
            return dict.getOrCreateTags(tags, (idx, t) -> addedTags.add(encodeTagDictionaryDelta(this, idx, t)));
        }

        public int getMetricName(MetricName metric) {
            return dict.getOrCreateMetricName(metric, (idx, m) -> addedMetricNames.add(encodePathDictionaryDelta(idx, m)));
        }

        public int getString(String str) {
            return dict.getOrCreateString(str, (idx, s) -> addedStrings.add(encodeStrvalDictionaryDelta(idx, s)));
        }

        public dictionary_delta encodedForm() {
            if (addedGroups.isEmpty() && addedTags.isEmpty() && addedMetricNames.isEmpty() && addedStrings.isEmpty())
                return null;

            dictionary_delta dd = new dictionary_delta();
            dd.gdd = addedGroups.stream().toArray(path_dictionary_delta[]::new);
            dd.tdd = addedTags.stream().toArray(tag_dictionary_delta[]::new);
            dd.mdd = addedMetricNames.stream().toArray(path_dictionary_delta[]::new);
            dd.sdd = addedStrings.stream().toArray(strval_dictionary_delta[]::new);
            return dd;
        }
    }

    private static class ActiveDict {
        private final BiMap<Integer, SimpleGroupPath> group_dict = HashBiMap.create();
        private final BiMap<Integer, Tags> tag_dict = HashBiMap.create();
        private final BiMap<Integer, MetricName> metric_dict = HashBiMap.create();
        private final BiMap<Integer, String> strval_dict = HashBiMap.create();

        public int getOrCreateGroup(SimpleGroupPath group, BiConsumer<Integer, SimpleGroupPath> onCreate) {
            return getOrCreate(group_dict, group, onCreate);
        }

        public int getOrCreateTags(Tags tags, BiConsumer<Integer, Tags> onCreate) {
            return getOrCreate(tag_dict, tags, onCreate);
        }

        public int getOrCreateMetricName(MetricName metric, BiConsumer<Integer, MetricName> onCreate) {
            return getOrCreate(metric_dict, metric, onCreate);
        }

        public int getOrCreateString(String str, BiConsumer<Integer, String> onCreate) {
            return getOrCreate(strval_dict, str, onCreate);
        }

        public SimpleGroupPath getGroup(int idx) {
            return group_dict.get(idx);
        }

        public Tags getTags(int idx) {
            return tag_dict.get(idx);
        }

        public MetricName getMetricName(int idx) {
            return metric_dict.get(idx);
        }

        public String getString(int idx) {
            return strval_dict.get(idx);
        }

        public void apply(@NonNull dictionary_delta dd) {
            Arrays.stream(dd.sdd)
                    .forEach(sdd -> strval_dict.put(sdd.id, sdd.value));
            Arrays.stream(dd.gdd)
                    .map(pdd -> decodePathDictionaryDelta(pdd, SimpleGroupPath::valueOf))
                    .forEach(entry -> group_dict.put(entry.getKey(), entry.getValue()));
            Arrays.stream(dd.mdd)
                    .map(mdd -> decodePathDictionaryDelta(mdd, MetricName::valueOf))
                    .forEach(entry -> metric_dict.put(entry.getKey(), entry.getValue()));
            Arrays.stream(dd.tdd)
                    .map(tdd -> decodeTagDictionaryDelta(this, tdd))
                    .forEach(entry -> tag_dict.put(entry.getKey(), entry.getValue()));
        }

        private static <T> int getOrCreate(BiMap<Integer, T> dict, T v, BiConsumer<Integer, T> onCreate) {
            {
                final Integer idx = dict.inverse().get(v);
                if (idx != null) return idx;
            }

            final int newIdx = allocateNext(dict);
            onCreate.accept(newIdx, v);
            dict.put(newIdx, v);
            return newIdx;
        }

        private static int allocateNext(BiMap<Integer, ?> map) {
            return map.keySet().stream()
                    .mapToInt(i -> i + 1)
                    .max()
                    .orElse(0);
        }
    }

    private static path_dictionary_delta encodePathDictionaryDelta(int idx, SimpleGroupPath group) {
        path_dictionary_delta pdd = new path_dictionary_delta();
        pdd.id = idx;
        pdd.value = new path();
        pdd.value.elems = group.getPath().stream()
                .map(path_elem::new)
                .toArray(path_elem[]::new);
        return pdd;
    }

    private static tag_dictionary_delta encodeTagDictionaryDelta(DictDelta dd, int idx, Tags tags) {
        tag_dictionary_delta tdd = new tag_dictionary_delta();
        tdd.id = idx;
        tdd.value = new tags();
        tdd.value.elems = tags.stream()
                .map(tagEntry -> {
                    tag_elem te = new tag_elem();
                    te.key = tagEntry.getKey();
                    te.value = encodeMetricValue(dd, tagEntry.getValue());
                    return te;
                })
                .toArray(tag_elem[]::new);
        return tdd;
    }

    private static Map.Entry<Integer, Tags> decodeTagDictionaryDelta(ActiveDict dd, tag_dictionary_delta tdd) {
        return SimpleMapEntry.create(tdd.id, Tags.valueOf(Arrays.stream(tdd.value.elems)
                .collect(Collectors.toMap(te -> te.key, te -> decodeMetricValue(dd, te.value)))));
    }

    private static path_dictionary_delta encodePathDictionaryDelta(int idx, MetricName metric) {
        path_dictionary_delta pdd = new path_dictionary_delta();
        pdd.id = idx;
        pdd.value = new path();
        pdd.value.elems = metric.getPath().stream()
                .map(path_elem::new)
                .toArray(path_elem[]::new);
        return pdd;
    }

    private static strval_dictionary_delta encodeStrvalDictionaryDelta(int idx, String str) {
        strval_dictionary_delta sdd = new strval_dictionary_delta();
        sdd.id = idx;
        sdd.value = str;
        return sdd;
    }

    private static <T> Map.Entry<Integer, T> decodePathDictionaryDelta(path_dictionary_delta pdd, Function<List<String>, T> converter) {
        return SimpleMapEntry.create(pdd.id, converter.apply(Arrays.stream(pdd.value.elems)
                .map(pe -> pe.value)
                .collect(Collectors.toList())));
    }
}
