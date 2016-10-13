/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr.support.writer;

import java.io.IOException;
import java.io.OutputStream;
import static java.lang.Math.min;
import java.nio.ByteBuffer;
import lombok.NonNull;
import org.iq80.snappy.SnappyFramedOutputStream;

/**
 *
 * @author ariane
 */
public class SnappyWriter implements FileWriter {
    private final SnappyFramedOutputStream snappy;

    public SnappyWriter(@NonNull FileWriter out) throws IOException {
        this.snappy = new SnappyFramedOutputStream(new Adapter(out));
    }

    @Override
    public int write(ByteBuffer data) throws IOException {
        if (data.hasArray()) {
            final int wlen = data.remaining();
            snappy.write(data.array(), data.arrayOffset() + data.position(), wlen);
            data.position(data.limit());
            return wlen;
        } else {
            int written = 0;
            byte buf[] = new byte[512];
            while (data.hasRemaining()) {
                final int buflen = min(data.remaining(), buf.length);
                data.get(buf, 0, buflen);
                snappy.write(buf, 0, buflen);
                written += buflen;
            }
            return written;
        }
    }

    @Override
    public void close() throws IOException {
        snappy.close();
    }

    @Override
    public ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    private static class Adapter extends OutputStream {
        private final FileWriter out;

        public Adapter(@NonNull FileWriter out) {
            this.out = out;
        }

        @Override
        public void write(int b) throws IOException {
            byte tmp[] = new byte[1];
            tmp[0] = (byte) (b & 0xff);
            write(tmp);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            ByteBuffer buf = ByteBuffer.wrap(b, off, len);
            while (buf.hasRemaining())
                out.write(buf);
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }
}
