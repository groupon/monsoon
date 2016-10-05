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
package com.groupon.lex.metrics.history.xdr.support.reader;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ForkJoinPool;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@AllArgsConstructor
public class FileChannelReader implements FileReader {
    @NonNull
    private final FileChannel fd;
    private long offset;

    @Override
    public int read(ByteBuffer data) throws IOException {
        if (offset >= fd.size())
            throw new EOFException("no more data (file end)");

        final int rlen = fjpRead(data);
        offset += rlen;
        return rlen;
    }

    @Override
    public void close() {}

    @Override
    public ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocateDirect(size);
    }

    @RequiredArgsConstructor
    private class FJPReader implements ForkJoinPool.ManagedBlocker {
        private int read = 0;
        private final ByteBuffer buf;
        private IOException ex;

        public int get() throws IOException {
            if (ex != null) throw ex;
            return read;
        }

        @Override
        public boolean block() throws InterruptedException {
            try {
                read += fd.read(buf, offset);
            } catch (IOException ex) {
                this.ex = ex;
            }
            return true;
        }

        @Override
        public boolean isReleasable() {
            return false;
        }
    }

    private int fjpRead(ByteBuffer buf) throws IOException {
        final FJPReader reader = new FJPReader(buf);
        try {
            ForkJoinPool.managedBlock(reader);
        } catch (InterruptedException ex) {
            throw new IOException("interrupted write", ex);
        }
        return reader.get();
    }
}
