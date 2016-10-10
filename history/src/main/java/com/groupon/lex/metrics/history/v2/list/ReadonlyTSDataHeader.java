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

import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.record_array;
import com.groupon.lex.metrics.history.v2.xdr.tsdata;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelSegmentReader;
import com.groupon.lex.metrics.history.xdr.support.reader.SegmentReader;
import com.groupon.lex.metrics.lib.GCCloseable;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class ReadonlyTSDataHeader {
    private static final Logger LOG = Logger.getLogger(ReadonlyTSDataHeader.class.getName());
    private final long ts;
    private final FilePos dictionary, records, previousTSData;

    public ReadonlyTSDataHeader(tsdata tsd) {
        this.ts = FromXdr.timestamp(tsd.ts).getMillis();
        this.dictionary = (tsd.dict == null ? null : FromXdr.filePos(tsd.dict));
        this.records = FromXdr.filePos(tsd.records);
        this.previousTSData = (tsd.previous == null ? null : FromXdr.filePos(tsd.previous));

        LOG.log(Level.FINER, "previous at {0}, dictionary at {1}, records at {2}", new Object[]{this.previousTSData, this.dictionary, this.records});
    }

    public DateTime getTimestamp() {
        return new DateTime(ts, DateTimeZone.UTC);
    }

    public Optional<FilePos> previousOffset() {
        return Optional.ofNullable(previousTSData);
    }

    public SegmentReader<Optional<dictionary_delta>> dictionaryDecoder(GCCloseable<FileChannel> file, boolean compressed) {
        if (dictionary == null) {
            LOG.log(Level.FINER, "no dictionary present");
            return SegmentReader.of(Optional.empty());
        }
        LOG.log(Level.FINER, "dictionary present at {0}", dictionary);
        return new FileChannelSegmentReader<>(dictionary_delta::new, file, dictionary, compressed)
                .map(Optional::of);
    }

    public SegmentReader<record_array> recordsDecoder(GCCloseable<FileChannel> file, boolean compressed) {
        return new FileChannelSegmentReader<>(record_array::new, file, records, compressed);
    }
}
