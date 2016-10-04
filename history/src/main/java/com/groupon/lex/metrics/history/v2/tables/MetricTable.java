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

import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.history.v2.ExportMap;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import com.groupon.lex.metrics.history.v2.xdr.histogram;
import com.groupon.lex.metrics.history.v2.xdr.metric_table;
import com.groupon.lex.metrics.history.v2.xdr.metric_value;
import com.groupon.lex.metrics.history.v2.xdr.mt_16bit;
import com.groupon.lex.metrics.history.v2.xdr.mt_32bit;
import com.groupon.lex.metrics.history.v2.xdr.mt_64bit;
import com.groupon.lex.metrics.history.v2.xdr.mt_bool;
import com.groupon.lex.metrics.history.v2.xdr.mt_dbl;
import com.groupon.lex.metrics.history.v2.xdr.mt_empty;
import com.groupon.lex.metrics.history.v2.xdr.mt_hist;
import com.groupon.lex.metrics.history.v2.xdr.mt_other;
import com.groupon.lex.metrics.history.v2.xdr.mt_str;
import gnu.trove.map.TIntByteMap;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TIntShortMap;
import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TIntShortHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import java.util.Arrays;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.joda.time.DateTime;
import org.joda.time.Duration;

@RequiredArgsConstructor
public class MetricTable extends AbstractSegmentWriter {
    @NonNull
    private final DateTime begin;
    @NonNull
    private final ExportMap<String> stringTable;

    private final TIntByteMap t_bool = new TIntByteHashMap();
    private final TIntShortMap t_16bit = new TIntShortHashMap();
    private final TIntIntMap t_32bit = new TIntIntHashMap();
    private final TIntLongMap t_64bit = new TIntLongHashMap();
    private final TIntDoubleMap t_dbl = new TIntDoubleHashMap();
    private final TIntIntMap t_str = new TIntIntHashMap();
    private final TIntObjectMap<Histogram> t_hist = new TIntObjectHashMap<>();
    private final TIntSet t_empty = new TIntHashSet();
    private final TIntObjectMap<MetricValue> t_other = new TIntObjectHashMap();

    public void add(@NonNull DateTime ts, @NonNull MetricValue value) {
        final long time_offset_long = new Duration(begin, ts).getMillis();
        if (time_offset_long < 0 || time_offset_long > Integer.MAX_VALUE)
            throw new IllegalArgumentException("ts out of range");
        final int time_offset = (int)time_offset_long;

        if (value.getBoolValue() != null) {
            t_bool.put(time_offset, value.getBoolValue() ? (byte)1 : (byte)0);
        } else if (value.getIntValue() != null) {
            final long v = value.getIntValue();
            if (v >= Short.MIN_VALUE && v <= Short.MAX_VALUE)
                t_16bit.put(time_offset, (short)v);
            else if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE)
                t_32bit.put(time_offset, (int)v);
            else
                t_64bit.put(time_offset, v);
        } else if (value.getFltValue() != null) {
            t_dbl.put(time_offset, value.getFltValue());
        } else if (value.getStrValue() != null) {
            t_str.put(time_offset, stringTable.getOrCreate(value.getStrValue()));
        } else if (value.getHistValue() != null) {
            t_hist.put(time_offset, value.getHistValue());
        } else if (value.isPresent()) {
            t_other.put(time_offset, value);
        } else {
            t_empty.add(time_offset);
        }
    }

    @Override
    public metric_table encode() {
        metric_table mt = new metric_table();
        mt.metrics_bool = encodeBool(t_bool);
        mt.metrics_16bit = encode16Bit(t_16bit);
        mt.metrics_32bit = encode32Bit(t_32bit);
        mt.metrics_64bit = encode64Bit(t_64bit);
        mt.metrics_dbl = encodeDbl(t_dbl);
        mt.metrics_str = encodeStr(t_str);
        mt.metrics_hist = encodeHist(t_hist);
        mt.metrics_empty = encodeEmpty(t_empty);
        mt.metrics_other = encodeOther(t_other, stringTable);
        return mt;
    }

    private static mt_bool encodeBool(TIntByteMap t_bool) {
        mt_bool result = new mt_bool();
        result.timestamp_delta = t_bool.keys();
        Arrays.sort(result.timestamp_delta);

        result.values = new boolean[result.timestamp_delta.length];
        for (int i = 0; i < result.timestamp_delta.length; ++i)
            result.values[i] = (t_bool.get(result.timestamp_delta[i]) != 0);

        return result;
    }

    private static mt_16bit encode16Bit(TIntShortMap t_16bit) {
        mt_16bit result = new mt_16bit();
        result.timestamp_delta = t_16bit.keys();
        Arrays.sort(result.timestamp_delta);

        result.values = new short[result.timestamp_delta.length];
        for (int i = 0; i < result.timestamp_delta.length; ++i)
            result.values[i] = t_16bit.get(result.timestamp_delta[i]);

        return result;
    }

    private static mt_32bit encode32Bit(TIntIntMap t_32bit) {
        mt_32bit result = new mt_32bit();
        result.timestamp_delta = t_32bit.keys();
        Arrays.sort(result.timestamp_delta);

        result.values = new int[result.timestamp_delta.length];
        for (int i = 0; i < result.timestamp_delta.length; ++i)
            result.values[i] = t_32bit.get(result.timestamp_delta[i]);

        return result;
    }

    private static mt_64bit encode64Bit(TIntLongMap t_64bit) {
        mt_64bit result = new mt_64bit();
        result.timestamp_delta = t_64bit.keys();
        Arrays.sort(result.timestamp_delta);

        result.values = new long[result.timestamp_delta.length];
        for (int i = 0; i < result.timestamp_delta.length; ++i)
            result.values[i] = t_64bit.get(result.timestamp_delta[i]);

        return result;
    }

    private static mt_dbl encodeDbl(TIntDoubleMap t_dbl) {
        mt_dbl result = new mt_dbl();
        result.timestamp_delta = t_dbl.keys();
        Arrays.sort(result.timestamp_delta);

        result.values = new double[result.timestamp_delta.length];
        for (int i = 0; i < result.timestamp_delta.length; ++i)
            result.values[i] = t_dbl.get(result.timestamp_delta[i]);

        return result;
    }

    private static mt_str encodeStr(TIntIntMap t_str) {
        mt_str result = new mt_str();
        result.timestamp_delta = t_str.keys();
        Arrays.sort(result.timestamp_delta);

        result.values = new int[result.timestamp_delta.length];
        for (int i = 0; i < result.timestamp_delta.length; ++i)
            result.values[i] = t_str.get(result.timestamp_delta[i]);

        return result;
    }

    private static mt_hist encodeHist(TIntObjectMap<Histogram> t_hist) {
        mt_hist result = new mt_hist();
        result.timestamp_delta = t_hist.keys();
        Arrays.sort(result.timestamp_delta);

        result.values = new histogram[result.timestamp_delta.length];
        for (int i = 0; i < result.timestamp_delta.length; ++i)
            result.values[i] = ToXdr.histogram(t_hist.get(result.timestamp_delta[i]));

        return result;
    }

    private static mt_empty encodeEmpty(TIntSet t_empty) {
        mt_empty result = new mt_empty();
        result.timestamp_delta = t_empty.toArray();
        Arrays.sort(result.timestamp_delta);
        return result;
    }

    private static mt_other encodeOther(TIntObjectMap<MetricValue> t_other, ExportMap<String> stringTable) {
        mt_other result = new mt_other();
        result.timestamp_delta = t_other.keys();
        Arrays.sort(result.timestamp_delta);

        result.values = new metric_value[result.timestamp_delta.length];
        for (int i = 0; i < result.timestamp_delta.length; ++i)
            result.values[i] = ToXdr.metricValue(t_other.get(result.timestamp_delta[i]), stringTable::getOrCreate);

        return result;
    }
}
