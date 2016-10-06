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

import static com.groupon.lex.metrics.history.xdr.Const.MIME_HEADER_LEN;
import static com.groupon.lex.metrics.history.xdr.support.ByteCountingXdrEncodingStream.xdrSize;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.FileTimeSeriesCollection;
import com.groupon.lex.metrics.history.xdr.support.ForwardSequence;
import com.groupon.lex.metrics.history.xdr.support.ObjectSequence;
import static com.groupon.lex.metrics.history.xdr.support.reader.Crc32Reader.CRC_LEN;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Util {
    /** Describe the length of the version 3 header. */
    public static final int HDR_3_LEN;
    /** Describe the length of a tsdata record header. */
    public static final int TSDATA_HDR_LEN;
    /** Number of bytes used by mime header + version 3 header + crc. */
    public static final int ALL_HDR_CRC_LEN;

    static {
        {
            tsfile_header sample = new tsfile_header();
            sample.fdt = ToXdr.filePos(new FilePos(0, 0));
            sample.file_size = 0;
            sample.first = ToXdr.timestamp(0);
            sample.last = ToXdr.timestamp(0);
            sample.flags = 0;
            sample.reserved = 0;
            HDR_3_LEN = (int)xdrSize(sample);
        }

        {
            tsdata sample = new tsdata();
            sample.ts = ToXdr.timestamp(0);
            sample.dd_len = 0;
            sample.r_len = 0;
            sample.reserved = 0;
            TSDATA_HDR_LEN = (int)xdrSize(sample);
        }

        ALL_HDR_CRC_LEN = MIME_HEADER_LEN + HDR_3_LEN + CRC_LEN;
    }

    public static long segmentLength(long dataLength) {
        return ((dataLength + 3) & ~3l) + CRC_LEN;
    }

    /** Fix a sequence to contain only unique, sorted elements. */
    public static ObjectSequence<TimeSeriesCollection> fixSequence(ObjectSequence<TimeSeriesCollection> seq) {
        if (seq.isSorted() && seq.isDistinct()) return seq;

        final List<TimeSeriesCollection> collection = seq.stream().collect(Collectors.toList());
        if (!seq.isSorted())
            Collections.sort(collection);

        if (!seq.isDistinct() && !collection.isEmpty()) {
            int curIdx = 0;

            while (curIdx < collection.size()) {
                final TimeSeriesCollection curElem = collection.get(curIdx);

                // Find first item with different timestamp.
                int nextIdx = curIdx + 1;
                while (nextIdx < collection.size() &&
                        curElem.getTimestamp().equals(collection.get(nextIdx).getTimestamp()))
                    ++nextIdx;

                // If more than 1 adjecent element share timestamp, merge them together.
                if (curIdx + 1 < nextIdx) {
                    TimeSeriesCollection replacement = new FileTimeSeriesCollection(curElem.getTimestamp(), collection.subList(curIdx, nextIdx).stream()
                            .flatMap(tsc -> tsc.getTSValues().stream())
                            .collect(Collectors.toMap(TimeSeriesValue::getGroup, Function.identity(), (x, y) -> x))
                            .values()
                            .stream());
                    collection.set(curIdx, replacement);  // Replace current element.
                    collection.subList(curIdx + 1, nextIdx).clear();  // Drop remaining duplicate elements.
                }

                // Advance curIdx.
                ++curIdx;
            }
        }

        return new ForwardSequence(0, collection.size())
                .map(collection::get, true, true, true);
    }
}
