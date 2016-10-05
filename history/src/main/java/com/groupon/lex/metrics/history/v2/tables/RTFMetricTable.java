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

import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.metric_table;
import com.groupon.lex.metrics.history.v2.xdr.mt_16bit;
import com.groupon.lex.metrics.history.v2.xdr.mt_32bit;
import com.groupon.lex.metrics.history.v2.xdr.mt_64bit;
import com.groupon.lex.metrics.history.v2.xdr.mt_bool;
import com.groupon.lex.metrics.history.v2.xdr.mt_dbl;
import com.groupon.lex.metrics.history.v2.xdr.mt_empty;
import com.groupon.lex.metrics.history.v2.xdr.mt_hist;
import com.groupon.lex.metrics.history.v2.xdr.mt_other;
import com.groupon.lex.metrics.history.v2.xdr.mt_str;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public class RTFMetricTable {
    private final metric_table input;
    private final DateTime begin;
    private final DictionaryDelta dictionary;

    public RTFMetricTable(metric_table input, DateTime begin, DictionaryDelta dictionary) {
        this.begin = begin;
        this.dictionary = dictionary;
        this.input = input;
    }

    public Stream<DateTime> getTimestamps() {
        return Stream.of(input.metrics_bool.timestamp_delta,
                         input.metrics_16bit.timestamp_delta,
                         input.metrics_32bit.timestamp_delta,
                         input.metrics_64bit.timestamp_delta,
                         input.metrics_dbl.timestamp_delta,
                         input.metrics_str.timestamp_delta,
                         input.metrics_hist.timestamp_delta,
                         input.metrics_empty.timestamp_delta,
                         input.metrics_other.timestamp_delta)
                .flatMapToInt(Arrays::stream)
                .mapToObj(Duration::new)
                .map(begin::plus);
    }

    public DateTime minTimestamp() {
        final OptionalInt v = Stream.of(input.metrics_bool.timestamp_delta,
                                        input.metrics_16bit.timestamp_delta,
                                        input.metrics_32bit.timestamp_delta,
                                        input.metrics_64bit.timestamp_delta,
                                        input.metrics_dbl.timestamp_delta,
                                        input.metrics_str.timestamp_delta,
                                        input.metrics_hist.timestamp_delta,
                                        input.metrics_empty.timestamp_delta,
                                        input.metrics_other.timestamp_delta)
                .flatMapToInt(arr -> {
                    return arr.length == 0 ? IntStream.empty() : IntStream.of(arr[0]);
                })
                .min();
        if (!v.isPresent()) return null;
        return begin.plus(new Duration(v));
    }

    public DateTime maxTimestamp() {
        final OptionalInt v = Stream.of(input.metrics_bool.timestamp_delta,
                                        input.metrics_16bit.timestamp_delta,
                                        input.metrics_32bit.timestamp_delta,
                                        input.metrics_64bit.timestamp_delta,
                                        input.metrics_dbl.timestamp_delta,
                                        input.metrics_str.timestamp_delta,
                                        input.metrics_hist.timestamp_delta,
                                        input.metrics_empty.timestamp_delta,
                                        input.metrics_other.timestamp_delta)
                .flatMapToInt(arr -> {
                    return arr.length == 0 ? IntStream.empty() : IntStream.of(arr[arr.length - 1]);
                })
                .max();
        if (!v.isPresent()) return null;
        return begin.plus(new Duration(v));
    }

    public MetricValue find(DateTime ts) {
        if (ts.isBefore(begin)) return null;
        final long tsOffset_long = new Duration(begin, ts).getMillis();
        if (tsOffset_long > Integer.MAX_VALUE) return null;

        return find((int)tsOffset_long);
    }

    public Optional<MetricValue> find(long ts) {
        if (ts < begin.getMillis()) return Optional.empty();
        final long tsOffset_long = ts - begin.getMillis();
        if (tsOffset_long > Integer.MAX_VALUE) return Optional.empty();

        return Optional.ofNullable(find((int)tsOffset_long));
    }

    public boolean isPresent(long ts) {
        if (ts < begin.getMillis()) return false;
        final long tsOffset_long = ts - begin.getMillis();
        if (tsOffset_long > Integer.MAX_VALUE) return false;

        return contains((int)tsOffset_long);
    }

    public boolean contains(DateTime ts) {
        if (ts.isBefore(begin)) return false;
        final long tsOffset_long = new Duration(begin, ts).getMillis();
        if (tsOffset_long > Integer.MAX_VALUE) return false;

        return contains((int)tsOffset_long);
    }

    private boolean contains(int tsOffset) {
        return  Arrays.binarySearch(input.metrics_bool.timestamp_delta, tsOffset) >= 0 ||
                Arrays.binarySearch(input.metrics_16bit.timestamp_delta, tsOffset) >= 0 ||
                Arrays.binarySearch(input.metrics_32bit.timestamp_delta, tsOffset) >= 0 ||
                Arrays.binarySearch(input.metrics_64bit.timestamp_delta, tsOffset) >= 0 ||
                Arrays.binarySearch(input.metrics_dbl.timestamp_delta, tsOffset) >= 0 ||
                Arrays.binarySearch(input.metrics_str.timestamp_delta, tsOffset) >= 0 ||
                Arrays.binarySearch(input.metrics_hist.timestamp_delta, tsOffset) >= 0 ||
                Arrays.binarySearch(input.metrics_empty.timestamp_delta, tsOffset) >= 0 ||
                Arrays.binarySearch(input.metrics_other.timestamp_delta, tsOffset) >= 0;
    }

    private MetricValue find(int tsOffset) {
        MetricValue mv;

        mv = find(input.metrics_bool, tsOffset);
        if (mv != null) return mv;

        mv = find(input.metrics_16bit, tsOffset);
        if (mv != null) return mv;

        mv = find(input.metrics_32bit, tsOffset);
        if (mv != null) return mv;

        mv = find(input.metrics_64bit, tsOffset);
        if (mv != null) return mv;

        mv = find(input.metrics_dbl, tsOffset);
        if (mv != null) return mv;

        mv = find(input.metrics_str, tsOffset, dictionary);
        if (mv != null) return mv;

        mv = find(input.metrics_hist, tsOffset);
        if (mv != null) return mv;

        mv = find(input.metrics_empty, tsOffset);
        if (mv != null) return mv;

        mv = find(input.metrics_other, tsOffset, dictionary);
        if (mv != null) return mv;

        return null;
    }

    private static MetricValue find(mt_bool entries, int tsOffset) {
        final int idx = Arrays.binarySearch(entries.timestamp_delta, tsOffset);
        if (idx >= 0) return MetricValue.fromBoolean(entries.values[idx]);
        return null;
    }

    private static MetricValue find(mt_16bit entries, int tsOffset) {
        final int idx = Arrays.binarySearch(entries.timestamp_delta, tsOffset);
        if (idx >= 0) return MetricValue.fromIntValue(entries.values[idx]);
        return null;
    }

    private static MetricValue find(mt_32bit entries, int tsOffset) {
        final int idx = Arrays.binarySearch(entries.timestamp_delta, tsOffset);
        if (idx >= 0) return MetricValue.fromIntValue(entries.values[idx]);
        return null;
    }

    private static MetricValue find(mt_64bit entries, int tsOffset) {
        final int idx = Arrays.binarySearch(entries.timestamp_delta, tsOffset);
        if (idx >= 0) return MetricValue.fromIntValue(entries.values[idx]);
        return null;
    }

    private static MetricValue find(mt_dbl entries, int tsOffset) {
        final int idx = Arrays.binarySearch(entries.timestamp_delta, tsOffset);
        if (idx >= 0) return MetricValue.fromDblValue(entries.values[idx]);
        return null;
    }

    private static MetricValue find(mt_str entries, int tsOffset, DictionaryDelta dictionary) {
        final int idx = Arrays.binarySearch(entries.timestamp_delta, tsOffset);
        if (idx >= 0) return MetricValue.fromStrValue(dictionary.getString(entries.values[idx]));
        return null;
    }

    private static MetricValue find(mt_hist entries, int tsOffset) {
        final int idx = Arrays.binarySearch(entries.timestamp_delta, tsOffset);
        if (idx >= 0) return MetricValue.fromHistValue(FromXdr.histogram(entries.values[idx]));
        return null;
    }

    private static MetricValue find(mt_empty entries, int tsOffset) {
        final int idx = Arrays.binarySearch(entries.timestamp_delta, tsOffset);
        if (idx >= 0) return MetricValue.EMPTY;
        return null;
    }

    private static MetricValue find(mt_other entries, int tsOffset, DictionaryDelta dictionary) {
        final int idx = Arrays.binarySearch(entries.timestamp_delta, tsOffset);
        if (idx >= 0) return FromXdr.metricValue(entries.values[idx], dictionary::getString);
        return null;
    }
}
