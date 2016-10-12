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
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPInputStream;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;
import org.junit.Before;
import org.junit.Test;

public class GzipWriterTest {
    private File file;
    private byte data[];

    @Before
    public void setup() throws Exception {
        file = File.createTempFile("monsoon-", "-GzipWriter");
        file.deleteOnExit();

        data = new byte[1024 * 1024];
        for (int i = 0; i < data.length; ++i)
            data[i] = (byte)(i ^ (i % 97));
    }

    @Test
    public void write() throws Exception {
        byte output[] = new byte[data.length];

        try (FileChannel fd = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
            try (FileWriter writer = new GzipWriter(new FileChannelWriter(fd, 0))) {
                ByteBuffer buf = ByteBuffer.wrap(data);
                while (buf.hasRemaining())
                    writer.write(buf);
            }
        }

        try (InputStream input = new GZIPInputStream(new FileInputStream(file))) {
            int off = 0;
            while (off < output.length) {
                int rlen = input.read(output, off, output.length - off);
                assertNotEquals("insufficient bytes were written", -1, rlen);
                off += rlen;
            }
        }

        assertArrayEquals(data, output);
    }

    @Test
    public void writeDirect() throws Exception {
        byte output[] = new byte[data.length];

        try (FileChannel fd = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
            try (FileWriter writer = new GzipWriter(new FileChannelWriter(fd, 0))) {
                ByteBuffer buf = ByteBuffer.allocateDirect(data.length);
                buf.put(data);
                buf.flip();
                while (buf.hasRemaining())
                    writer.write(buf);
            }
        }

        try (InputStream input = new GZIPInputStream(new FileInputStream(file))) {
            int off = 0;
            while (off < output.length) {
                int rlen = input.read(output, off, output.length - off);
                assertNotEquals("insufficient bytes were written", -1, rlen);
                off += rlen;
            }
        }

        assertArrayEquals(data, output);
    }
}
