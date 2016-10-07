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

import java.io.EOFException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

public class FileChannelReaderTest {
    private FileChannel file;
    private ByteBuffer data;

    @Before
    public void setup() throws Exception {
        data = ByteBuffer.allocate(8);
        data.order(ByteOrder.BIG_ENDIAN);
        data.putInt(17);
        data.putInt(19);
        data.flip();

        File fileName = File.createTempFile("monsoon-", "-FileChannelReaderTest");
        fileName.deleteOnExit();
        file = FileChannel.open(fileName.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        while (data.hasRemaining())
            file.write(data, 0);

        data.position(0);
        data.limit(8);
    }

    @Test
    public void read() throws Exception {
        try (FileChannelReader reader = new FileChannelReader(file, 0)) {
            ByteBuffer buf = reader.allocateByteBuffer(32);
            buf.order(ByteOrder.BIG_ENDIAN);

            int rlen = reader.read(buf);
            buf.flip();

            assertEquals(8, rlen);
            assertEquals(8, buf.limit());
            assertEquals(data.getInt(), buf.getInt());
            assertEquals(data.getInt(), buf.getInt());
        }
    }

    /**
     * Closing FileChannelReader will not close FileChannel.
     */
    @Test
    public void close() throws Exception {
        FileChannelReader reader = new FileChannelReader(file, 0);
        reader.close();
        assertTrue(file.isOpen());
    }

    @Test
    public void readAtPos() throws Exception {
        try (FileChannelReader reader = new FileChannelReader(file, 4)) {
            ByteBuffer buf = reader.allocateByteBuffer(32);
            buf.order(ByteOrder.BIG_ENDIAN);

            int rlen = reader.read(buf);
            buf.flip();

            assertEquals(4, rlen);
            assertEquals(4, buf.limit());
            assertEquals(19, buf.getInt());
        }
    }

    @Test(expected = EOFException.class)
    public void eof() throws Exception {
        try (FileChannelReader reader = new FileChannelReader(file, 4)) {
            ByteBuffer buf = reader.allocateByteBuffer(32);
            buf.order(ByteOrder.BIG_ENDIAN);

            while (buf.hasRemaining())
                reader.read(buf);
        }
    }

    @Test
    public void readContinue() throws Exception {
        try (FileChannelReader reader = new FileChannelReader(file, 0)) {
            ByteBuffer buf = reader.allocateByteBuffer(4);
            buf.order(ByteOrder.BIG_ENDIAN);

            int rlen = reader.read(buf);
            buf.flip();

            assertEquals(4, rlen);
            assertEquals(4, buf.limit());
            assertEquals(17, buf.getInt());

            buf.compact();
            rlen = reader.read(buf);
            buf.flip();

            assertEquals(4, rlen);
            assertEquals(4, buf.limit());
            assertEquals(19, buf.getInt());
        }
    }
}
