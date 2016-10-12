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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class Crc32AppendingFileWriterTest {
    private File file;
    private byte data[];
    private int expectedCrc;
    private final int padLen = 3;

    @Before
    public void setup() throws Exception {
        file = File.createTempFile("monsoon-", "-Crc32AppendingFileWriter");
        file.deleteOnExit();

        data = new byte[1024 * 1024 - padLen];
        for (int i = 0; i < data.length; ++i)
            data[i] = (byte)(i ^ (i % 97));

        CRC32 crc = new CRC32();
        crc.update(data);
        for (int i = 0; i < padLen; ++i) crc.update(0);
        expectedCrc = (int)crc.getValue();
    }

    @Test
    public void write() throws Exception {
        byte output[] = new byte[data.length];
        int crc;

        try (FileChannel fd = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            try (FileWriter writer = new Crc32AppendingFileWriter(new FileChannelWriter(fd, 0), 4)) {
                ByteBuffer buf = ByteBuffer.wrap(data);
                while (buf.hasRemaining())
                    writer.write(buf);
            }

            MappedByteBuffer mapped = fd.map(FileChannel.MapMode.READ_ONLY, 0, fd.size());
            mapped.order(ByteOrder.BIG_ENDIAN);
            mapped.get(output);
            for (int i = 0; i < padLen; ++i)
                assertEquals((byte)0, mapped.get());
            crc = mapped.getInt();
            assertFalse(mapped.hasRemaining());
        }

        assertArrayEquals(data, output);
        assertEquals(expectedCrc, crc);
    }
}
