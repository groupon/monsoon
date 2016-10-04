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
package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.history.xdr.BufferSupplier;
import com.groupon.lex.metrics.lib.GCCloseable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SegmentBufferSupplier implements BufferSupplier {
    private final GCCloseable<FileChannel> file;
    private long pos;
    private final long end;

    public SegmentBufferSupplier(GCCloseable<FileChannel> file, FilePos range) {
        this(file, range.getOffset(), range.getEnd());
    }

    @Override
    public void load(ByteBuffer buf) throws IOException {
        final int remaining = (int)Long.min(end - pos, Integer.MAX_VALUE);
        if (remaining == 0) return;

        if (buf.remaining() <= remaining) {
            pos += file.get().read(buf, pos);
        } else {
            final ByteBuffer tmp = buf.duplicate();
            tmp.limit(tmp.position() + remaining);
            pos += file.get().read(tmp, pos);
            buf.position(buf.position() + remaining);
        }
    }

    @Override
    public boolean atEof() throws IOException {
        return pos == end;
    }
}
