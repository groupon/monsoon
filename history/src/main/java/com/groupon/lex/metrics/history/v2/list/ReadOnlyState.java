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
package com.groupon.lex.metrics.history.v2.list;

import com.groupon.lex.metrics.history.v2.tables.DictionaryDelta;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import com.groupon.lex.metrics.history.v2.xdr.tsdata;
import com.groupon.lex.metrics.history.v2.xdr.tsfile_header;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.ForwardSequence;
import com.groupon.lex.metrics.history.xdr.support.ObjectSequence;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelSegmentReader;
import com.groupon.lex.metrics.history.xdr.support.reader.SegmentReader;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Getter;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;

public class ReadOnlyState implements State {
    private static final Logger LOG = Logger.getLogger(ReadOnlyState.class.getName());
    /** Cached TSData headers. */
    private final List<SegmentReader<TimeSeriesCollection>> tsdata;
    @Getter
    private final GCCloseable<FileChannel> file;
    @Getter
    private final DateTime begin, end;
    private final boolean sorted;
    private final boolean distinct;

    public ReadOnlyState(GCCloseable<FileChannel> file, tsfile_header hdr) throws IOException, OncRpcException {
        this.file = file;
        final boolean gzipped = (hdr.flags & header_flags.GZIP) == header_flags.GZIP;
        this.sorted = (hdr.flags & header_flags.SORTED) == header_flags.SORTED;
        this.distinct = (hdr.flags & header_flags.DISTINCT) == header_flags.DISTINCT;
        this.begin = FromXdr.timestamp(hdr.first);
        this.end = FromXdr.timestamp(hdr.last);

        final List<SegmentReader<ReadonlyTSDataHeader>> tsdataHeaders = readAllTSDataHeaders(file, FromXdr.filePos(hdr.fdt));
        final SegmentReader<DictionaryDelta> dictionary = calculateDictionary(file, gzipped, tsdataHeaders).cache();
        this.tsdata = unmodifiableList(calculateTimeSeries(file, gzipped, tsdataHeaders, dictionary));
    }

    @Override
    public ObjectSequence<SegmentReader<TimeSeriesCollection>> sequence() {
        return new ForwardSequence(0, tsdata.size())
                .map(tsdata::get, sorted, true, distinct);
    }

    @Override
    public void add(TimeSeriesCollection tsc) {
        throw new UnsupportedOperationException("read-only file");
    }

    @Override
    public void addAll(Collection<? extends TimeSeriesCollection> tsc) {
        throw new UnsupportedOperationException("read-only file");
    }

    public static List<SegmentReader<ReadonlyTSDataHeader>> readAllTSDataHeaders(GCCloseable<FileChannel> file, FilePos recordPos) throws IOException, OncRpcException {
        final ArrayList<SegmentReader<ReadonlyTSDataHeader>> headers = new ArrayList<>();
        if (recordPos.getOffset() == 0) return headers;  // Empty list.

        while (recordPos != null) {
            final SegmentReader<ReadonlyTSDataHeader> tsd = readTSDataHeader(file, recordPos);
            final ReadonlyTSDataHeader hdr = tsd.decode();
            recordPos = hdr.previousOffset().orElse(null);
            headers.add(tsd);
        }

        headers.trimToSize();
        Collections.reverse(headers);  // We read headers from last (most recently added) to first (least recently added), reversing makes the ordering make sense.
        return headers;
    }

    public static SegmentReader<ReadonlyTSDataHeader> readTSDataHeader(GCCloseable<FileChannel> file, FilePos pos) {
        LOG.log(Level.FINEST, "new ReadonlyTSDataHeader segment at {0}", pos);
        return new FileChannelSegmentReader<>(tsdata::new, file, pos, false)
                .map(ReadonlyTSDataHeader::new)
                .cache();
    }

    public static SegmentReader<DictionaryDelta> calculateDictionary(GCCloseable<FileChannel> file, boolean compressed, List<SegmentReader<ReadonlyTSDataHeader>> tsdataList) {
        SegmentReader<DictionaryDelta> accumulated = SegmentReader.of(new DictionaryDelta());
        for (SegmentReader<ReadonlyTSDataHeader> tsdata : tsdataList) {
            accumulated = tsdata
                    .flatMap(tsdHeader -> tsdHeader.dictionaryDecoder(file, compressed))
                    .combine(accumulated, (optTsd, dict) -> optTsd.map(tsd -> new DictionaryDelta(tsd, dict)).orElse(dict));
        }
        return accumulated;
    }

    public static List<SegmentReader<TimeSeriesCollection>> calculateTimeSeries(GCCloseable<FileChannel> file, boolean compressed, List<SegmentReader<ReadonlyTSDataHeader>> tsdataList, SegmentReader<DictionaryDelta> dictionary) {
        final FileChannelSegmentReader.Factory segmentFactory = new FileChannelSegmentReader.Factory(file, compressed);
        ArrayList<SegmentReader<TimeSeriesCollection>> result = new ArrayList<>();
        for (SegmentReader<ReadonlyTSDataHeader> tsdata : tsdataList) {
            result.add(tsdata
                    .map(tsdHeader -> {
                        final TimeSeriesCollection tsc = new ListTSC(tsdHeader.getTimestamp(), tsdHeader.recordsDecoder(file, compressed), dictionary, segmentFactory);
                        return tsc;
                    })
                    .share());
        }
        result.trimToSize();
        return result;
    }
}
