package com.groupon.lex.metrics.history.xdr.support.reader;

import com.groupon.lex.metrics.history.xdr.support.IOLengthVerificationFailed;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import lombok.NonNull;
import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoDecompressor;
import org.anarres.lzo.LzoInputStream;
import org.anarres.lzo.LzoLibrary;

public class LzoReader implements FileReader {
    public static final LzoAlgorithm ALGORITHM = LzoAlgorithm.LZO1X;
    private final LzoInputStream lzo;
    private final boolean validateAllRead;

    public LzoReader(FileReader in, boolean validateAllRead) {
        LzoDecompressor decompressor = LzoLibrary.getInstance().newDecompressor(ALGORITHM, null);
        this.lzo = new LzoInputStream(new Adapter(in), decompressor);
        this.lzo.setInputBufferSize(64 * 1024);
        this.validateAllRead = validateAllRead;
    }

    public LzoReader(FileReader in) {
        this(in, true);
    }

    @Override
    public int read(ByteBuffer data) throws IOException {
        final int rlen;
        if (data.hasArray()) {
            rlen = lzo.read(data.array(), data.arrayOffset() + data.position(), data.remaining());
            if (rlen > 0)
                data.position(data.position() + rlen);
        } else {
            byte tmp[] = new byte[data.remaining()];
            rlen = lzo.read(tmp);
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
            if (lzo.read() != -1)
                throw new IOLengthVerificationFailed(0, 0);
        }
        lzo.close();
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
