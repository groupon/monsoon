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
package com.groupon.lex.metrics.history.v2.xdr;

import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricValue;
import gnu.trove.list.TShortList;
import gnu.trove.list.array.TShortArrayList;
import java.util.Arrays;
import java.util.function.Function;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author ariane
 */
public class ToXdr {
    public static timestamp_msec timestamp(DateTime ts) {
        timestamp_msec result = new timestamp_msec();
        result.value = ts.toDateTime(DateTimeZone.UTC).getMillis();
        return result;
    }

    public static metric_value metricValue(MetricValue mv, Function<String, Integer> strAlloc) {
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
            result.str_dict_ref = strAlloc.apply(mv.getStrValue());
        }
        if (mv.getHistValue() != null) {
            result.kind = metrickind.HISTOGRAM;
            result.hist_value = histogram(mv.getHistValue());
        }

        return result;
    }

    public static histogram histogram(Histogram h) {
        return new histogram(h.stream()
                .map(he -> {
                    final histogram_entry encoded = new histogram_entry();
                    encoded.floor = he.getRange().getFloor();
                    encoded.ceil = he.getRange().getCeil();
                    encoded.events = he.getCount();
                    return encoded;
                })
                .toArray(histogram_entry[]::new));
    }

    public static timestamp_delta timestamp_delta(long begin, long[] timestamps) {
        timestamps = Arrays.copyOf(timestamps, timestamps.length);
        Arrays.sort(timestamps);
        if (timestamps.length >= 1 && timestamps[0] < begin)
            throw new RuntimeException("encoding error: negative time delta");

        int[] result = new int[timestamps.length];
        for (int i = 0; i < timestamps.length; ++i) {
            final long delta = timestamps[i] - begin;
            if (delta > Integer.MAX_VALUE)
                throw new RuntimeException("encoding error: time delta too large");
            result[i] = (int)delta;
            begin = timestamps[i];
        }
        return new timestamp_delta(result);
    }

    public static bitset bitset(boolean bits[]) {
        TShortList resultList = new TShortArrayList();
        boolean expect = true;

        int bitsIdx = 0;
        while (bitsIdx < bits.length) {
            int end = bitsIdx;
            while (end < bits.length && bits[end] == expect) ++end;

            int len = end - bitsIdx;
            while (len > 65535) {
                bitsIdx += 65535;
                len -= 65535;
                resultList.add((short)0xffff);
                resultList.add((short)0);
            }

            resultList.add((short)len);
            bitsIdx += len;

            expect = !expect;
        }

        return new bitset(resultList.toArray());
    }
}
