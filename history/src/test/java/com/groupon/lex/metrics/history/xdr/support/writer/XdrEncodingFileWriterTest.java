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

import java.io.File;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

public class XdrEncodingFileWriterTest {
    private File file;
    private int[] ints;
    private byte[] bytes;

    @Before
    public void setup() throws Exception {
        file = File.createTempFile("monsoon-", "-XdrEncodingFileWriter");
        file.deleteOnExit();

        ints = new int[256 * 1024];
        for (int i = 0; i < ints.length; ++i)
            ints[i] = (i ^ (i % 97)) * 65537;

        bytes = new byte[1024 * 1024];
        for (int i = 0; i < bytes.length; ++i)
            bytes[i] = (byte)(i ^ i % 97);
    }

    @Test
    public void writeInts() throws Exception {
        int output[] = new int[ints.length];

        try (FileChannel fd = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            try (XdrEncodingFileWriter writer = new XdrEncodingFileWriter(new FileChannelWriter(fd, 0))) {
                writer.beginEncoding();
                writer.xdrEncodeIntFixedVector(ints, ints.length);
                writer.endEncoding();
            }

            MappedByteBuffer map = fd.map(FileChannel.MapMode.READ_ONLY, 0, fd.size());
            map.order(ByteOrder.BIG_ENDIAN);
            for (int i = 0; i < output.length; ++i)
                output[i] = map.getInt();

            assertFalse(map.hasRemaining());
        }

        assertArrayEquals(ints, output);
    }

    @Test
    public void writeBytes() throws Exception {
        byte output[] = new byte[bytes.length];

        try (FileChannel fd = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            try (XdrEncodingFileWriter writer = new XdrEncodingFileWriter(new FileChannelWriter(fd, 0))) {
                writer.beginEncoding();
                writer.xdrEncodeOpaque(bytes, bytes.length);
                writer.endEncoding();
            }

            MappedByteBuffer map = fd.map(FileChannel.MapMode.READ_ONLY, 0, fd.size());
            map.order(ByteOrder.BIG_ENDIAN);
            map.get(output);

            assertFalse(map.hasRemaining());
        }

        assertArrayEquals(bytes, output);
    }
}
