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

import java.io.Closeable;
import java.io.IOException;
import static java.lang.Math.min;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import lombok.NonNull;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrDecodingStream;

public class XdrDecodingFileReader extends XdrDecodingStream implements Closeable {
    private final FileReader in;
    private final ByteBuffer buf;

    public XdrDecodingFileReader(@NonNull FileReader in, int bufSiz) {
        if (bufSiz < 4) bufSiz = 4;
        this.in = in;
        buf = in.allocateByteBuffer(bufSiz);
        buf.order(ByteOrder.BIG_ENDIAN);
    }

    public XdrDecodingFileReader(@NonNull FileReader in) {
        this(in, 64 * 1024);
    }

    @Override
    public void beginDecoding() {}
    @Override
    public void endDecoding() {}

    @Override
    public InetAddress getSenderAddress() { return InetAddress.getLoopbackAddress(); }
    @Override
    public int getSenderPort() { return 0; }

    @Override
    public void close() throws IOException { in.close(); }

    @Override
    public int xdrDecodeInt() throws OncRpcException, IOException {
        while (buf.remaining() < 4) readMore();
        return buf.getInt();
    }

    @Override
    public byte[] xdrDecodeOpaque(int length) throws OncRpcException, IOException {
        byte[] b = new byte[length];
        xdrDecodeOpaque(b, 0, length);
        return b;
    }

    @Override
    public void xdrDecodeOpaque(byte[] opaque, int offset, int length) throws OncRpcException, IOException {
        int pad_len = length % 4 == 0 ? 0 : 4 - length % 4;

        while (length > 0) {
            if (!buf.hasRemaining()) readMore();
            final int rdlen = min(length, buf.remaining());
            buf.get(opaque, offset, rdlen);

            offset += rdlen;
            length -= rdlen;
        }

        while (pad_len > 0) {
            if (!buf.hasRemaining()) readMore();
            final int inc = min(pad_len, buf.remaining());
            buf.position(buf.position() + inc);
            pad_len -= inc;
        }
    }

    private void readMore() throws IOException {
        buf.compact();
        in.read(buf);
        buf.flip();
    }
}
