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
package com.groupon.lex.metrics.history.xdr.support.writer;

import com.groupon.lex.metrics.history.xdr.support.FilePos;
import java.io.IOException;
import java.nio.channels.FileChannel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;

public abstract class AbstractSegmentWriter {
    public abstract XdrAble encode(long timestamps[]);

    public FilePos write(Writer writer, long timestamps[]) throws OncRpcException, IOException {
        return writer.write(encode(timestamps));
    }

    @AllArgsConstructor
    public static class Writer {
        @NonNull
        private final FileChannelWriter out;
        private final boolean compress;

        public Writer(@NonNull FileChannel out, long offset, boolean compress) {
            this.out = new FileChannelWriter(out, offset);
            this.compress = compress;
        }

        public FilePos write(XdrAble object) throws IOException, OncRpcException {
            final long initPos;
            if (compress)
                initPos = (out.getOffset() + 3) & ~3l;
            else
                initPos = (out.getOffset() + 7) & ~7l;
            out.setOffset(initPos);

            try (Crc32AppendingFileWriter outerWriter = new Crc32AppendingFileWriter(new CloseInhibitingWriter(out), 4)) {
                try (XdrEncodingFileWriter writer = new XdrEncodingFileWriter(wrapWriter(new CloseInhibitingWriter(outerWriter), compress))) {
                    writer.beginEncoding();
                    object.xdrEncode(writer);
                    writer.endEncoding();
                }

                return new FilePos(initPos, outerWriter.getWritten());
            }
        }

        private static FileWriter wrapWriter(FileWriter underlying, boolean compress) throws IOException {
            FileWriter result = underlying;
            if (compress) result = new GzipWriter(result);
            return result;
        }
    }
}
