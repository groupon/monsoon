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
import java.nio.ByteBuffer;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SizeVerifyingReader implements FileReader {
    private final FileReader in;
    private final long expected;
    private long read = 0;

    @Override
    public int read(ByteBuffer data) throws IOException {
        if (read == expected)
            throw new EOFException("no more data");
        if (data.remaining() > expected - read)
            data.limit(data.position() + (int) (expected - read));

        assert (data.remaining() <= expected - read);
        final int rlen = in.read(data);
        read += rlen;
        return rlen;
    }

    @Override
    public void close() throws IOException {
        if (read != expected)
            throw new IOLengthVerificationFailed(expected, read);
        in.close();
    }

    @Override
    public ByteBuffer allocateByteBuffer(int size) {
        return in.allocateByteBuffer(size);
    }
}
