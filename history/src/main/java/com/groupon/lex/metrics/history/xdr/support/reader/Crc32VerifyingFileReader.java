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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import lombok.Getter;
import lombok.NonNull;

public class Crc32VerifyingFileReader implements FileReader {
    public static final int CRC_LEN = Crc32Reader.CRC_LEN;  // CRC32 is 4 bytes.
    private final Crc32Reader check;  // Used to get CRC.
    private final FileReader forRead;  // Enforces size of caller to read and prevents accidentally handing out padding bytes.
    private final FileReader underlying;  // Underlying reader.
    private final int padLen;

    public Crc32VerifyingFileReader(@NonNull FileReader in, long len, int roundUp) {
        if (roundUp < 0) throw new IllegalArgumentException("cannot round up to negative values");
        if (roundUp == 0) roundUp = 1;
        this.underlying = in;
        if (len % roundUp == 0)
            this.padLen = 0;
        else
            this.padLen = roundUp - (int)(len % roundUp);
        assert((len + padLen) % roundUp == 0);
        this.check = new Crc32Reader(new SizeVerifyingReader(new CloseInhibitingReader(this.underlying), len + padLen));
        this.forRead = new SizeVerifyingReader(new CloseInhibitingReader(this.check), len);
    }

    @Override
    public int read(ByteBuffer data) throws IOException {
        return forRead.read(data);
    }

    @Override
    public void close() throws IOException {
        forRead.close();  // Verify caller of read() consumed all data.
        consumePadding();
        check.close();  // Close check after consuming data, verifies we used up len+padLen that we said we would.
        final int readCRC = check.getCrc32();

        // Read CRC from underlying reader.
        final ByteBuffer tmp = underlying.allocateByteBuffer(CRC_LEN);
        tmp.order(ByteOrder.BIG_ENDIAN);
        while (tmp.hasRemaining()) underlying.read(tmp);
        tmp.flip();

        // Verify actual CRC equals expected CRC.
        final int expectedCRC = tmp.getInt();
        if (expectedCRC != readCRC) throw new IOCrcMismatchException(readCRC, expectedCRC);

        underlying.close();  // Finally close underlying reader.
    }

    @Override
    public ByteBuffer allocateByteBuffer(int size) {
        return check.allocateByteBuffer(size);
    }

    private void consumePadding() throws IOException {
        final ByteBuffer buf = check.allocateByteBuffer(padLen);
        buf.order(ByteOrder.BIG_ENDIAN);
        while (buf.hasRemaining()) check.read(buf);
        buf.flip();

        for (int i = 0; i < padLen; ++i) {
            if (buf.get() != (byte)0)
                throw new IOPaddingException();
        }
    }

    public static class IOCrcMismatchException extends IOException {
        @Getter
        private final int read, expected;

        public IOCrcMismatchException(int read, int expected) {
            super("CRC32 mismatch");
            this.read = read;
            this.expected = expected;
        }
    }

    public static class IOPaddingException extends IOException {
        public IOPaddingException() {
            super("padding bytes incorrect");
        }
    }
}
