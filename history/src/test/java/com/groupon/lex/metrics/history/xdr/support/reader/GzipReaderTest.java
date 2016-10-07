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
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Before;
import org.junit.Test;

public class GzipReaderTest {
    private FileChannel file;
    private byte expected[];

    @Before
    public void setup() throws Exception {
        expected = new byte[1024 * 1024];
        for (int i = 0; i < expected.length; ++i)
            expected[i] = (byte)(i ^ (i % 97) & 0xff);

        File fileName = File.createTempFile("monsoon-", "-GzipReaderTest");
        fileName.deleteOnExit();

        try (OutputStream out = new GZIPOutputStream(new FileOutputStream(fileName))) {
            out.write(expected);
        }

        file = FileChannel.open(fileName.toPath(), StandardOpenOption.READ);
    }

    @Test
    public void read() throws Exception {
        byte output[] = new byte[expected.length];

        try (FileReader reader = new GzipReader(new FileChannelReader(file, 0))) {
            int i = 0;
            while (i < output.length) {
                final int rlen = reader.read(ByteBuffer.wrap(output, i, Integer.min(output.length - i, 128)));

                byte[] expectedChunk = Arrays.copyOfRange(expected, i, i + rlen);
                byte[] outputChunk = Arrays.copyOfRange(output, i, i + rlen);
                assertArrayEquals(expectedChunk, outputChunk);

                i += rlen;
            }
        }

        assertArrayEquals(expected, output);
    }
}
