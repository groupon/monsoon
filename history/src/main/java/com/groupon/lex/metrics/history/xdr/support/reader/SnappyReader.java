package com.groupon.lex.metrics.history.xdr.support.reader;

import com.groupon.lex.metrics.history.xdr.support.IOLengthVerificationFailed;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import lombok.NonNull;
import org.iq80.snappy.SnappyFramedInputStream;

public class SnappyReader implements FileReader {
    private final SnappyFramedInputStream snappy;
    private final boolean validateAllRead;

    public SnappyReader(FileReader in, boolean validateAllRead) throws IOException {
        this.snappy = new SnappyFramedInputStream(new Adapter(in), true);
        this.validateAllRead = validateAllRead;
    }

    public SnappyReader(FileReader in) throws IOException {
        this(in, true);
    }

    @Override
    public int read(ByteBuffer data) throws IOException {
        final int rlen;
        if (data.hasArray()) {
            rlen = snappy.read(data.array(), data.arrayOffset() + data.position(), data.remaining());
            if (rlen > 0)
                data.position(data.position() + rlen);
        } else {
            byte tmp[] = new byte[data.remaining()];
            rlen = snappy.read(tmp);
            if (rlen > 0)
                data.put(tmp, 0, rlen);
        }

        if (rlen == -1)
            throw new EOFException("no more data (gzip)");
        return rlen;
    }

    @Override
    public void close() throws IOException {
        if (validateAllRead) {
            if (snappy.read() != -1)
                throw new IOLengthVerificationFailed(0, 0);
        }
        snappy.close();
    }

    @Override
    public ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    private static class Adapter extends InputStream {
        private final FileReader in;

        public Adapter(@NonNull FileReader in) {
            this.in = in;
        }

        @Override
        public int read() throws IOException {
            byte tmp[] = new byte[1];
            int rlen = read(tmp);
            if (rlen == -1)
                return -1;
            return (int) tmp[0] & 0xff;
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            if (len == 0)
                return 0;

            int rlen;
            try {
                rlen = in.read(ByteBuffer.wrap(b, off, len));
            } catch (EOFException ex) {
                rlen = -1;
            }
            return rlen;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
