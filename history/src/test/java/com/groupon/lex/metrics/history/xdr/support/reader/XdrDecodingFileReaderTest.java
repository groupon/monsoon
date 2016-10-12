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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class XdrDecodingFileReaderTest {
    private static final int EXPECT_INT = 19;
    private static byte EXPECT_OPAQUE[];

    private Path file;

    static {
        EXPECT_OPAQUE = new byte[32 - 4 - 4];
        for (int i = 0; i < EXPECT_OPAQUE.length; ++i)
            EXPECT_OPAQUE[i] = (byte)i;
    }

    @Before
    public void setup() throws Exception {
        File fileName = File.createTempFile("monsoon-", "-XdrDecodingFileReader");
        fileName.deleteOnExit();
        file = fileName.toPath();

        try (FileChannel fd = FileChannel.open(file, StandardOpenOption.WRITE)) {
            ByteBuffer test = ByteBuffer.allocate(32);
            test.order(ByteOrder.BIG_ENDIAN);

            test.putInt(EXPECT_INT);

            // Opaque buffer, filled with byte value 19.
            test.putInt(EXPECT_OPAQUE.length);
            test.put(EXPECT_OPAQUE);

            test.flip();
            fd.write(test);
        }
    }

    @Test
    public void decode() throws Exception {
        try (FileChannel fd = FileChannel.open(file, StandardOpenOption.READ)) {
            try (XdrDecodingFileReader reader = new XdrDecodingFileReader(new FileChannelReader(fd, 0))) {
                reader.beginDecoding();
                assertEquals(EXPECT_INT, reader.xdrDecodeInt());
                assertArrayEquals(EXPECT_OPAQUE, reader.xdrDecodeDynamicOpaque());
                reader.endDecoding();
            }
        }
    }
}
