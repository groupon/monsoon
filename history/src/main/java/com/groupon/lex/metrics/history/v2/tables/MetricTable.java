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
import gnu.trove.TDecorators;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.NonNull;

public class MetricTable {
    private static final Logger LOG = Logger.getLogger(MetricTable.class.getName());

    private final int timestampsSize;
    private final TIntByteMap t_bool;
    private final TIntShortMap t_16bit;
    private final TIntIntMap t_32bit;
    private final TIntLongMap t_64bit;
    private final TIntDoubleMap t_dbl;
    private final TIntIntMap t_str;
    private final TIntObjectMap<histogram> t_hist;
    private final TIntSet t_empty;
    private final TIntObjectMap<metric_value> t_other;

    public MetricTable(int timestampsSize) {
        this.timestampsSize = timestampsSize;
        this.t_bool = new TIntByteHashMap(timestampsSize, 1, -1, (byte) -1);
        this.t_16bit = new TIntShortHashMap(timestampsSize, 1, -1, (short) -1);
        this.t_32bit = new TIntIntHashMap(timestampsSize, 1, -1, -1);
        this.t_64bit = new TIntLongHashMap(timestampsSize, 1, -1, -1);
        this.t_dbl = new TIntDoubleHashMap(timestampsSize, 1, -1, -1);
        this.t_str = new TIntIntHashMap(timestampsSize, 1, -1, -1);
        this.t_hist = new TIntObjectHashMap<>(timestampsSize, 1, -1);
        this.t_empty = new TIntHashSet(timestampsSize, 1, -1);
        this.t_other = new TIntObjectHashMap<>(timestampsSize, 1, -1);
    }

    public void add(int ts, @NonNull DictionaryForWrite dictionary, @NonNull MetricValue value) {
        add(ts, ToXdr.metricValue(value, dictionary.getStringTable()::getOrCreate));
    }

    public void add(int ts, @NonNull metric_value value) {
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

    public metric_table encode() {
        metric_table mt = new metric_table();
        mt.metrics_bool = encodeBool(t_bool, timestampsSize);
        mt.metrics_16bit = encode16Bit(t_16bit, timestampsSize);
        mt.metrics_32bit = encode32Bit(t_32bit, timestampsSize);
        mt.metrics_64bit = encode64Bit(t_64bit, timestampsSize);
        mt.metrics_dbl = encodeDbl(t_dbl, timestampsSize);
        mt.metrics_str = encodeStr(t_str, timestampsSize);
        mt.metrics_hist = encodeHist(t_hist, timestampsSize);
        mt.metrics_empty = encodeEmpty(t_empty, timestampsSize);
        mt.metrics_other = encodeOther(t_other, timestampsSize);
        return mt;
    }

    private static mt_bool encodeBool(TIntByteMap t_bool, int timestampsSize) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_bool));
        boolean values[] = new boolean[timestampsSize];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            if (t_bool.containsKey(i))
                values[values_len++] = t_bool.get(i) != 0;
        }

        mt_bool result = new mt_bool();
        result.presence = createPresenceBitset(t_bool.keySet(), timestampsSize);
        result.values = bitset(Arrays.copyOf(values, values_len));

        return result;
    }

    private static mt_16bit encode16Bit(TIntShortMap t_16bit, int timestampsSize) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_16bit));
        short[] values = new short[timestampsSize];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            if (t_16bit.containsKey(i))
                values[values_len++] = t_16bit.get(i);
        }

        mt_16bit result = new mt_16bit();
        result.presence = createPresenceBitset(t_16bit.keySet(), timestampsSize);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }

    private static mt_32bit encode32Bit(TIntIntMap t_32bit, int timestampsSize) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_32bit));
        int[] values = new int[timestampsSize];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            if (t_32bit.containsKey(i))
                values[values_len++] = t_32bit.get(i);
        }

        mt_32bit result = new mt_32bit();
        result.presence = createPresenceBitset(t_32bit.keySet(), timestampsSize);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }

    private static mt_64bit encode64Bit(TIntLongMap t_64bit, int timestampsSize) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_64bit));
        long[] values = new long[timestampsSize];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            if (t_64bit.containsKey(i))
                values[values_len++] = t_64bit.get(i);
        }

        mt_64bit result = new mt_64bit();
        result.presence = createPresenceBitset(t_64bit.keySet(), timestampsSize);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }

    private static mt_dbl encodeDbl(TIntDoubleMap t_dbl, int timestampsSize) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_dbl));
        double[] values = new double[timestampsSize];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            if (t_dbl.containsKey(i))
                values[values_len++] = t_dbl.get(i);
        }

        mt_dbl result = new mt_dbl();
        result.presence = createPresenceBitset(t_dbl.keySet(), timestampsSize);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }

    private static mt_str encodeStr(TIntIntMap t_str, int timestampsSize) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_str));
        int[] values = new int[timestampsSize];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            if (t_str.containsKey(i))
                values[values_len++] = t_str.get(i);
        }

        mt_str result = new mt_str();
        result.presence = createPresenceBitset(t_str.keySet(), timestampsSize);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }

    private static mt_hist encodeHist(TIntObjectMap<histogram> t_hist, int timestampsSize) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_hist));
        histogram values[] = new histogram[timestampsSize];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            if (t_hist.containsKey(i))
                values[values_len++] = t_hist.get(i);
        }

        mt_hist result = new mt_hist();
        result.presence = createPresenceBitset(t_hist.keySet(), timestampsSize);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }

    private static mt_empty encodeEmpty(TIntSet t_empty, int timestampsSize) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_empty));
        mt_empty result = new mt_empty();
        result.presence = createPresenceBitset(t_empty, timestampsSize);
        return result;
    }

    private static mt_other encodeOther(TIntObjectMap<metric_value> t_other, int timestampsSize) {
        LOG.log(Level.FINEST, "encoding {0}", TDecorators.wrap(t_other));
        metric_value[] values = new metric_value[timestampsSize];
        int values_len = 0;
        for (int i = 0; i < values.length; ++i) {
            metric_value mv = t_other.get(i);
            if (mv != null)
                values[values_len++] = mv;
        }

        mt_other result = new mt_other();
        result.presence = createPresenceBitset(t_other.keySet(), timestampsSize);
        result.values = Arrays.copyOf(values, values_len);

        return result;
    }
}
