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
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import java.util.Arrays;
import java.util.function.IntFunction;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class FromXdr {
    public static DateTime timestamp(timestamp_msec ts) {
        return new DateTime(ts.value, DateTimeZone.UTC);
    }

    public static FilePos filePos(file_segment fs) {
        return new FilePos(fs.offset, fs.len);
    }

    public static MetricValue metricValue(metric_value mv, IntFunction<String> strGet) {
        switch (mv.kind) {
            default:
                throw new IllegalStateException("unrecognized metric kind: " + mv.kind);
            case metrickind.EMPTY:
                return MetricValue.EMPTY;
            case metrickind.BOOL:
                return MetricValue.fromBoolean(mv.bool_value);
            case metrickind.INT:
                return MetricValue.fromIntValue(mv.int_value);
            case metrickind.FLOAT:
                return MetricValue.fromDblValue(mv.dbl_value);
            case metrickind.STRING:
                return MetricValue.fromStrValue(strGet.apply(mv.str_dict_ref));
            case metrickind.HISTOGRAM:
                return MetricValue.fromHistValue(histogram(mv.hist_value));
        }
    }

    public static Histogram histogram(histogram h) {
        return new Histogram(Arrays.stream(h.value)
                .map(he -> new Histogram.RangeWithCount(he.floor, he.ceil, he.events)));
    }

    public static long[] timestamp_delta(long begin, timestamp_delta tsd) {
        long[] result = new long[tsd.value.length];
        for (int i = 0; i < tsd.value.length; ++i) {
            begin += tsd.value[i];
            result[i] = begin;
        }
        return result;
    }

    public static boolean[] bitset(bitset b) {
        int len = 0;
        for (int i = 0; i < b.value.length; ++i)
            len += (int)b.value[i] & 0xffff;
        final boolean result[] = new boolean[len];

        boolean resultVal = true;
        int resultIdx = 0;
        for (int i = 0; i < b.value.length; ++i) {
            int count = (int)b.value[i] & 0xffff;
            Arrays.fill(result, resultIdx, count, resultVal);

            resultIdx += count;
            resultVal = !resultVal;
        }

        assert(resultIdx == result.length);
        return result;
    }
}
