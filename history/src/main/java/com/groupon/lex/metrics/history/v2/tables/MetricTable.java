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
import com.groupon.lex.metrics.history.v2.DictionaryForWrite;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import static com.groupon.lex.metrics.history.v2.xdr.ToXdr.bitset;
import static com.groupon.lex.metrics.history.v2.xdr.ToXdr.createPresenceBitset;
import com.groupon.lex.metrics.history.v2.xdr.histogram;
import com.groupon.lex.metrics.history.v2.xdr.metric_table;
import com.groupon.lex.metrics.history.v2.xdr.metric_value;
import com.groupon.lex.metrics.history.v2.xdr.metrickind;
import com.groupon.lex.metrics.history.v2.xdr.mt_16bit;
import com.groupon.lex.metrics.history.v2.xdr.mt_32bit;
import com.groupon.lex.metrics.history.v2.xdr.mt_64bit;
import com.groupon.lex.metrics.history.v2.xdr.mt_bool;
import com.groupon.lex.metrics.history.v2.xdr.mt_dbl;
import com.groupon.lex.metrics.history.v2.xdr.mt_empty;
import com.groupon.lex.metrics.history.v2.xdr.mt_hist;
import com.groupon.lex.metrics.history.v2.xdr.mt_other;
import com.groupon.lex.metrics.history.v2.xdr.mt_str;
import com.groupon.lex.metrics.history.xdr.support.writer.AbstractSegmentWriter;
import gnu.trove.TDecorators;
import gnu.trove.map.TLongByteMap;
import gnu.trove.map.TLongDoubleMap;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.TLongShortMap;
import gnu.trove.map.hash.TLongByteHashMap;
import gnu.trove.map.hash.TLongDoubleHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TLongShortHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MetricTable extends AbstractSegmentWriter {
    private static final Logger LOG = Logger.getLogger(MetricTable.class.getName());

    @NonNull
    private final DictionaryForWrite dictionary;

    private final TLongByteMap t_bool = new TLongByteHashMap();
    private final TLongShortMap t_16bit = new TLongShortHashMap();
    private final TLongIntMap t_32bit = new TLongIntHashMap();
    private final TLongLongMap t_64bit = new TLongLongHashMap();
    private final TLongDoubleMap t_dbl = new TLongDoubleHashMap();
    private final TLongIntMap t_str = new TLongIntHashMap();
    private final TLongObjectMap<histogram> t_hist = new TLongObjectHashMap<>();
    private final TLongSet t_empty = new TLongHashSet();
    private final TLongObjectMap<metric_value> t_other = new TLongObjectHashMap<>();

    public void add(long ts, @NonNull MetricValue value) {
        add(ts, ToXdr.metricValue(value, dictionary.getStringTable()::getOrCreate));
    }

    public void add(long ts, @NonNull metric_value value) {
        switch (value.kind) {
            default:
                t_other.put(ts, value);
                break;
            case metrickind.BOOL:
                t_bool.put(ts, value.bool_value ? (byte) 1 : (byte) 0);
                break;
            case metrickind.INT:
                final long v = value.int_value;
                if (v >= Short.MIN_VALUE && v <= Short.MAX_VALUE)
                    t_16bit.put(ts, (short) v);
                else if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE)
                    t_32bit.put(ts, (int) v);
                else
                    t_64bit.put(ts, v);
                break;
            case metrickind.FLOAT:
                t_dbl.put(ts, value.dbl_value);
                break;
            case metrickind.STRING:
                t_str.put(ts, value.str_dict_ref);
                break;
            case metrickind.HISTOGRAM:
                t_hist.put(ts, value.hist_value);
                break;
            case metrickind.EMPTY:
                t_empty.add(ts);
                break;
        }
    }

    @Override
    public metric_table encode(long timestamps[]) {
        metric_table mt = new metric_table();
        mt.metrics_bool = encodeBool(t_bool, timestamps);
        mt.metrics_16bit = encode16Bit(t_16bit, timestamps);
        mt.metrics_32bit = encode32Bit(t_32bit, timestamps);
        mt.metrics_64bit = encode64Bit(t_64bit, timestamps);
        mt.metrics_dbl = encodeDbl(t_dbl, timestamps);
        mt.metrics_str = encodeStr(t_str, timestamps);
        mt.metrics_hist = encodeHist(t_hist, timestamps);
        mt.metrics_empty = encodeEmpty(t_empty, timestamps);
        mt.metrics_other = encodeOther(t_other, timestamps);
        return mt;
    }

    private static mt_bool encodeBool(TLongByteMap t_bool, long timestamps[]) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_bool));
        boolean values[] = new boolean[timestamps.length];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            final long ts = timestamps[i];
            if (t_bool.containsKey(ts))
                values[values_len++] = t_bool.get(ts) != 0;
        }

        mt_bool result = new mt_bool();
        result.presence = createPresenceBitset(t_bool.keySet(), timestamps);
        result.values = bitset(Arrays.copyOf(values, values_len));

        return result;
    }

    private static mt_16bit encode16Bit(TLongShortMap t_16bit, long timestamps[]) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_16bit));
        short[] values = new short[timestamps.length];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            final long ts = timestamps[i];
            if (t_16bit.containsKey(ts))
                values[values_len++] = t_16bit.get(ts);
        }

        mt_16bit result = new mt_16bit();
        result.presence = createPresenceBitset(t_16bit.keySet(), timestamps);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }

    private static mt_32bit encode32Bit(TLongIntMap t_32bit, long timestamps[]) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_32bit));
        int[] values = new int[timestamps.length];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            final long ts = timestamps[i];
            if (t_32bit.containsKey(ts))
                values[values_len++] = t_32bit.get(ts);
        }

        mt_32bit result = new mt_32bit();
        result.presence = createPresenceBitset(t_32bit.keySet(), timestamps);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }

    private static mt_64bit encode64Bit(TLongLongMap t_64bit, long timestamps[]) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_64bit));
        long[] values = new long[timestamps.length];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            final long ts = timestamps[i];
            if (t_64bit.containsKey(ts))
                values[values_len++] = t_64bit.get(ts);
        }

        mt_64bit result = new mt_64bit();
        result.presence = createPresenceBitset(t_64bit.keySet(), timestamps);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }

    private static mt_dbl encodeDbl(TLongDoubleMap t_dbl, long timestamps[]) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_dbl));
        double[] values = new double[timestamps.length];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            final long ts = timestamps[i];
            if (t_dbl.containsKey(ts))
                values[values_len++] = t_dbl.get(ts);
        }

        mt_dbl result = new mt_dbl();
        result.presence = createPresenceBitset(t_dbl.keySet(), timestamps);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }

    private static mt_str encodeStr(TLongIntMap t_str, long timestamps[]) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_str));
        int[] values = new int[timestamps.length];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            final long ts = timestamps[i];
            if (t_str.containsKey(ts))
                values[values_len++] = t_str.get(ts);
        }

        mt_str result = new mt_str();
        result.presence = createPresenceBitset(t_str.keySet(), timestamps);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }

    private static mt_hist encodeHist(TLongObjectMap<histogram> t_hist, long timestamps[]) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_hist));
        histogram values[] = new histogram[timestamps.length];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            final long ts = timestamps[i];
            if (t_hist.containsKey(ts))
                values[values_len++] = t_hist.get(ts);
        }

        mt_hist result = new mt_hist();
        result.presence = createPresenceBitset(t_hist.keySet(), timestamps);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }

    private static mt_empty encodeEmpty(TLongSet t_empty, long timestamps[]) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_empty));
        mt_empty result = new mt_empty();
        result.presence = createPresenceBitset(t_empty, timestamps);
        return result;
    }

    private static mt_other encodeOther(TLongObjectMap<metric_value> t_other, long timestamps[]) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_other));
        metric_value[] values = new metric_value[timestamps.length];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            final long ts = timestamps[i];
            metric_value mv = t_other.get(ts);
            if (mv != null)
                values[values_len++] = mv;
        }

        mt_other result = new mt_other();
        result.presence = createPresenceBitset(t_other.keySet(), timestamps);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }
}
