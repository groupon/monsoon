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

import java.io.IOException;
import static java.lang.Integer.max;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import lombok.Getter;
import lombok.NonNull;

public class Crc32AppendingFileWriter implements FileWriter {
    private final Crc32Writer out;
    @Getter
    private long written;
    @Getter
    private final int roundUp;

    public Crc32AppendingFileWriter(@NonNull FileWriter out, int roundUp) {
        if (roundUp < 0) throw new IllegalArgumentException("cannot round up to negative values");
        if (roundUp == 0) roundUp = 1;
        this.roundUp = roundUp;
        this.out = new Crc32Writer(out);
    }

    @Override
    public int write(ByteBuffer buf) throws IOException {
        final int wlen = out.write(buf);
        written += wlen;
        return wlen;
    }

    @Override
    public void close() throws IOException {
        final ByteBuffer buf = allocateByteBuffer(max(4, roundUp));
        buf.order(ByteOrder.BIG_ENDIAN);

        if (written % roundUp != 0) {
            final int padLen = roundUp - (int)(written % roundUp);
            for (int i = 0; i < padLen; ++i) buf.put((byte)0);
            buf.flip();
            while (buf.hasRemaining()) out.write(buf);
            buf.compact();
        }

        buf.putInt(out.getCrc32());
        buf.flip();
        while (buf.hasRemaining()) out.write(buf);

        out.close();
    }

    @Override
    public ByteBuffer allocateByteBuffer(int size) {
        return out.allocateByteBuffer(size);
    }
}
