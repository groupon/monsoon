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

import com.groupon.lex.metrics.history.xdr.support.IOLengthVerificationFailed;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import static java.util.Objects.requireNonNull;
import lombok.NonNull;

public class AbstractInputStreamReader implements FileReader {
    private final InputStream in;
    private final boolean validateAllRead;

    protected AbstractInputStreamReader(InputStream in, boolean validateAllRead) throws IOException {
        this.in = requireNonNull(in);
        this.validateAllRead = validateAllRead;
    }

    protected AbstractInputStreamReader(InputStream in) throws IOException {
        this(in, true);
    }

    @Override
    public int read(ByteBuffer data) throws IOException {
        final int rlen;
        if (data.hasArray()) {
            rlen = in.read(data.array(), data.arrayOffset() + data.position(), data.remaining());
            if (rlen > 0)
                data.position(data.position() + rlen);
        } else {
            byte tmp[] = new byte[data.remaining()];
            rlen = in.read(tmp);
            if (rlen > 0)
                data.put(tmp, 0, rlen);
        }

        if (rlen == -1)
            throw new EOFException("no more data");
        return rlen;
    }

    @Override
    public void close() throws IOException {
        if (validateAllRead) {
            if (in.read() != -1)
                throw new IOLengthVerificationFailed(0, 0);
        }
        in.close();
    }

    @Override
    public ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    protected static InputStream newAdapter(FileReader in) {
        return new Adapter(in);
    }

    private static class Adapter extends InputStream {
        private final FileReader in;

        public Adapter(@NonNull FileReader in) {
            this.in = in;
        }

        @Override
        public int read() throws IOException {
            byte tmp[] = new byte[1];
            int rlen = read(tmp);
            if (rlen == -1)
                return -1;
            return (int) tmp[0] & 0xff;
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            if (len == 0)
                return 0;

            int rlen;
            try {
                rlen = in.read(ByteBuffer.wrap(b, off, len));
            } catch (EOFException ex) {
                rlen = -1;
            }
            return rlen;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
