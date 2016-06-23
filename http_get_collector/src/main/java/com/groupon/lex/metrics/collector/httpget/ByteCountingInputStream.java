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
package com.groupon.lex.metrics.collector.httpget;

import java.io.IOException;
import java.io.InputStream;
import static java.util.Objects.requireNonNull;

/**
 * A wrapper around an input stream, that keeps track of how many bytes have been read.
 * @author ariane
 */
public class ByteCountingInputStream extends InputStream {
    private final InputStream in_;
    private long len_ = 0;
    private long reset_len_ = 0;
    private final boolean propagate_close_;

    public ByteCountingInputStream(InputStream in, boolean propagate_close) {
        in_ = requireNonNull(in);
        propagate_close_ = propagate_close;
    }

    public ByteCountingInputStream(InputStream in) {
        this(in, true);
    }

    public long getBytesRead() { return len_; }

    @Override
    public int read() throws IOException {
        int v = in_.read();
        if (v != -1) ++len_;
        return v;
    }

    @Override
    public int read(byte b[]) throws IOException {
        int c = in_.read(b);
        if (c != -1) len_ += c;
        return c;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int c = in_.read(b, off, len);
        if (c != -1) len_ += c;
        return c;
    }

    @Override
    public long skip(long n) throws IOException {
        long c = in_.skip(n);
        len_ += c;
        return c;
    }

    @Override
    public int available() throws IOException {
        return in_.available();
    }

    @Override
    public void close() throws IOException {
        if (propagate_close_) in_.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        in_.mark(readlimit);
        reset_len_ = len_;
    }

    @Override
    public synchronized void reset() throws IOException {
        in_.reset();
        len_ = reset_len_;
    }

    @Override
    public boolean markSupported() {
        return in_.markSupported();
    }
}
