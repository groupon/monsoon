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

import com.groupon.lex.metrics.history.xdr.support.IOLengthVerificationFailed;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class Crc32VerifyingFileReaderTest {
    private Path file;

    @Before
    public void setup() throws Exception {
        File fileName = File.createTempFile("monsoon-", "-Crc32Reader");
        fileName.deleteOnExit();
        file = fileName.toPath();
    }

    public byte[] setupWithCrc(byte padValue, boolean correctCrc, int padLen, int roundUp) throws Exception {
        assert(padLen == 0 ||
                (padLen > 0 && padLen < roundUp));  // Padding is used to reach multiple of roundUp.
        byte[] data;
        data = new byte[roundUp * (8 * 1024 + 1) - padLen];
        for (int i = 0; i < data.length; ++i)
            data[i] = (byte)(i ^ (i % 97));

        CRC32 crc32 = new CRC32();
        crc32.update(ByteBuffer.wrap(data));

        try (FileChannel fd = FileChannel.open(file, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.wrap(data);
            while (buf.hasRemaining())
                fd.write(buf);

            buf = ByteBuffer.allocate(padLen);
            buf.order(ByteOrder.BIG_ENDIAN);
            while (buf.hasRemaining()) buf.put(padValue);
            buf.flip();
            crc32.update(buf.duplicate());
            while (buf.hasRemaining())
                fd.write(buf);

            buf = ByteBuffer.allocate(4);
            buf.order(ByteOrder.BIG_ENDIAN);
            buf.putInt((int)(correctCrc ? crc32.getValue() : ~crc32.getValue()));
            buf.flip();
            while (buf.hasRemaining())
                fd.write(buf);
        }

        return data;
    }

    public void readValid(byte padValue, boolean correctCrc, int padLen, int roundUp) throws Exception {
        byte expected[] = setupWithCrc(padValue, correctCrc, padLen, roundUp);
        byte output[] = new byte[expected.length];

        try (FileChannel fd = FileChannel.open(file, StandardOpenOption.READ)) {
            try (FileReader reader = new Crc32VerifyingFileReader(new FileChannelReader(fd, 0), expected.length, roundUp)) {
                int i = 0;
                while (i < output.length)
                    i += reader.read(ByteBuffer.wrap(output, i, Integer.min(output.length - i, 128)));
            }
        }

        assertArrayEquals(expected, output);
    }

    @Test
    public void read_pad0_valid() throws Exception {
        readValid((byte)0, true, 0, 0);
    }

    @Test
    public void read_pad0_valid_roundUp() throws Exception {
        readValid((byte)0, true, 0, 7);  // Should still work with weird roundup.
    }

    @Test
    public void read_pad3_valid_roundUp() throws Exception {
        readValid((byte)0, true, 3, 7);
    }

    @Test
    public void read_pad6_valid_roundUp() throws Exception {
        readValid((byte)0, true, 6, 7);  // Should still work with weird roundup.
    }

    @Test(expected = Crc32VerifyingFileReader.IOCrcMismatchException.class)
    public void read_pad0_invalid() throws Exception {
        readValid((byte)0, false, 0, 0);
    }

    @Test(expected = Crc32VerifyingFileReader.IOCrcMismatchException.class)
    public void read_pad0_invalid_roundUp() throws Exception {
        readValid((byte)0, false, 3, 4);
    }

    @Test(expected = Crc32VerifyingFileReader.IOCrcMismatchException.class)
    public void read_pad3_invalid_roundUp() throws Exception {
        readValid((byte)0, false, 3, 4);
    }

    @Test(expected = Crc32VerifyingFileReader.IOPaddingException.class)
    public void read_badPad_valid() throws Exception {
        readValid((byte)7, false, 3, 4);
    }

    @Test(expected = IOException.class)
    public void read_badPad_invalid() throws Exception {
        readValid((byte)7, false, 3, 4);
    }

    @Test(expected = IOLengthVerificationFailed.class)
    public void readTooLittle() throws Exception {
        byte expected[] = setupWithCrc((byte)0, true, 0, 4);
        byte output[] = new byte[expected.length - 1024];

        try (FileChannel fd = FileChannel.open(file, StandardOpenOption.READ)) {
            try (FileReader reader = new Crc32VerifyingFileReader(new FileChannelReader(fd, 0), expected.length, 4)) {
                int i = 0;
                while (i < output.length)
                    i += reader.read(ByteBuffer.wrap(output, i, Integer.min(output.length - i, 128)));
            }
        }
    }

    @Test(expected = IOLengthVerificationFailed.class)
    public void readTooMuch() throws Exception {
        byte expected[] = setupWithCrc((byte)0, true, 0, 4);
        byte output[] = new byte[expected.length + 1024];

        try (FileChannel fd = FileChannel.open(file, StandardOpenOption.READ)) {
            try (FileReader reader = new Crc32VerifyingFileReader(new FileChannelReader(fd, 0), expected.length, 4)) {
                int i = 0;
                while (i < output.length)
                    i += reader.read(ByteBuffer.wrap(output, i, Integer.min(output.length - i, 128)));
            }
        }
    }
}
