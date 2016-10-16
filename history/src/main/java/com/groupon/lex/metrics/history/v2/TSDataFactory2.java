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
package com.groupon.lex.metrics.history.v2;

import com.groupon.lex.metrics.history.TSDataVersionDispatch;
import com.groupon.lex.metrics.history.v2.list.RWListFile;
import com.groupon.lex.metrics.history.v2.tables.ReadonlyTableFile;
import static com.groupon.lex.metrics.history.v2.xdr.Util.HDR_3_LEN;
import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import com.groupon.lex.metrics.history.v2.xdr.tsfile_header;
import static com.groupon.lex.metrics.history.xdr.Const.MIME_HEADER_LEN;
import com.groupon.lex.metrics.history.xdr.support.SequenceTSData;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelReader;
import com.groupon.lex.metrics.history.xdr.support.reader.XdrDecodingFileReader;
import com.groupon.lex.metrics.lib.GCCloseable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import org.acplt.oncrpc.OncRpcException;

/**
 * Contains an ordered list of factories that open files.
 *
 * Handles reading the mime header, verifying it and dispatching to a factory
 * based on the major version.
 */
public class TSDataFactory2 implements TSDataVersionDispatch.Factory {
    @Override
    public SequenceTSData open(TSDataVersionDispatch.Releaseable<FileChannel> fd, boolean completeGzipped) throws IOException {
        if (completeGzipped)
            throw new IOException("version 2.x files may not be gzipped");

        final tsfile_header hdr;
        try (XdrDecodingFileReader reader = new XdrDecodingFileReader(new FileChannelReader(fd.get(), MIME_HEADER_LEN), HDR_3_LEN)) {
            reader.beginDecoding();
            hdr = new tsfile_header(reader);
            reader.endDecoding();
        } catch (OncRpcException ex) {
            throw new IOException("error reading extended header", ex);
        }

        try {
            switch (hdr.flags & header_flags.KIND_MASK) {
                default:
                    throw new IOException("unrecognized encoding type");
                case header_flags.KIND_LIST:
                    return new RWListFile(new GCCloseable<>(fd.release()), false);
                case header_flags.KIND_TABLES:
                    return new ReadonlyTableFile(new GCCloseable<>(fd.release()));
            }
        } catch (OncRpcException ex) {
            throw new IOException("failed to load file", ex);
        }
    }
}
