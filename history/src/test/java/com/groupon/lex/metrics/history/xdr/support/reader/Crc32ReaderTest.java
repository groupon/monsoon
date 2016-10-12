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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class Crc32ReaderTest {
    private Path file;
    private int expectedCrc32;
    private byte[] data;

    @Before
    public void setup() throws Exception {
        File fileName = File.createTempFile("monsoon-", "-Crc32Reader");
        fileName.deleteOnExit();
        file = fileName.toPath();

        data = new byte[1024 * 1024];
        for (int i = 0; i < data.length; ++i)
            data[i] = (byte)(i ^ (i % 97));

        try (FileChannel fd = FileChannel.open(file, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.wrap(data);
            while (buf.hasRemaining())
                fd.write(buf);
        }

        CRC32 crc32 = new CRC32();
        crc32.update(ByteBuffer.wrap(data));
        expectedCrc32 = (int)crc32.getValue();
    }

    @Test
    public void read() throws Exception {
        byte output[] = new byte[data.length];
        int outputCrc;

        try (FileChannel fd = FileChannel.open(file, StandardOpenOption.READ)) {
            try (Crc32Reader reader = new Crc32Reader(new FileChannelReader(fd, 0))) {
                int i = 0;
                while (i < output.length)
                    i += reader.read(ByteBuffer.wrap(output, i, Integer.min(output.length - i, 128)));

                outputCrc = reader.getCrc32();
            }
        }

        assertEquals(expectedCrc32, outputCrc);
        assertArrayEquals(data, output);
    }
}
