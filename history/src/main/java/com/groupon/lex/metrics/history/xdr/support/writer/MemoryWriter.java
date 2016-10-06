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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import lombok.Getter;

/**
 * A file writer that only writes to memory buffers.
 */
public class MemoryWriter {
    @Getter
    private final List<ByteBuffer> output = new ArrayList<>();
    boolean closed = false;

    public List<ByteBuffer> getBuffers() {
        if (!closed) throw new IllegalStateException("cannot access these buffers until the stream closes");
        return unmodifiableList(output);
    }

    public FileWriter getWriter() { return new WriterImpl(); }

    private class WriterImpl implements FileWriter {
        @Override
        public int write(ByteBuffer data) throws IOException {
            if (closed) throw new IllegalStateException("cannot write after closing");

            int written = 0;
            while (data.hasRemaining()) {
                if (output.isEmpty() || !output.get(output.size() - 1).hasRemaining())
                    output.add(ByteBuffer.allocateDirect(65536));
                final ByteBuffer last = output.get(output.size() - 1);

                if (data.remaining() > last.remaining()) {
                    final int poppedLimit = data.limit();
                    data.limit(data.position() + last.remaining());
                    assert(data.remaining() <= last.remaining());
                    written += data.remaining();
                    data.put(last);
                    data.limit(poppedLimit);
                } else {
                    written += data.remaining();
                    data.put(last);
                }
            }
            return written;
        }

        @Override
        public ByteBuffer allocateByteBuffer(int size) {
            return ByteBuffer.allocate(1024);
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                output.forEach(ByteBuffer::flip);
                closed = true;
            }
        }
    }
}
