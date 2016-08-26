package org.dcache.xdr;

import com.groupon.lex.metrics.history.xdr.BufferSupplier;
import java.io.IOException;
import static java.lang.Math.max;
import static java.lang.Math.min;
import java.net.InetAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import static java.util.Objects.requireNonNull;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrDecodingStream;

/**
 *
 * @author ariane
 */
public class XdrBufferDecodingStream extends XdrDecodingStream {
    private static final int MIN_BUFSIZ = 8;
    private static final int DEFAULT_BUFSIZ = 4 * 1024;
    private final ByteBuffer buf_;
    private final BufferSupplier buf_supplier_;
    private long read_bytes_ = 0;

    public XdrBufferDecodingStream(ByteBuffer buf, BufferSupplier buf_supplier) throws IOException {
        buf_supplier_ = requireNonNull(buf_supplier);
        buf_ = requireNonNull(buf);
        if (buf_.isReadOnly())
            throw new IllegalArgumentException("may not supply read-only buffer if you use a buffer supplier");
        if (buf_.capacity() < MIN_BUFSIZ)
            throw new IllegalArgumentException("buffer too short, require at least " + MIN_BUFSIZ + " bytes");
        buf_.order(ByteOrder.BIG_ENDIAN);
        buf_supplier_.load(buf_);
        buf_.flip();
    }

    public XdrBufferDecodingStream(BufferSupplier buf_supplier) throws IOException {
        this(DEFAULT_BUFSIZ, buf_supplier);
    }

    public XdrBufferDecodingStream(int bufsize, BufferSupplier buf_supplier) throws IOException {
        this(ByteBuffer.allocate(bufsize), buf_supplier);
    }

    public XdrBufferDecodingStream(ByteBuffer buf) {
        buf_ = requireNonNull(buf);
        buf_.order(ByteOrder.BIG_ENDIAN);
        buf_supplier_ = null;
    }

    private void update_buffer_() throws OncRpcException, IOException {
        if (buf_supplier_ == null) return;
        read_bytes_ += buf_.position();
        buf_.compact();
        buf_supplier_.load(buf_);
        buf_.flip();
    }

    @Override
    public void beginDecoding() {}
    @Override
    public void endDecoding() {}
    @Override
    public InetAddress getSenderAddress() { return InetAddress.getLoopbackAddress(); }
    @Override
    public int getSenderPort() { return 0; }

    @Override
    public int xdrDecodeInt() throws OncRpcException, IOException {
        if (buf_.remaining() < 4) update_buffer_();
        try {
            return buf_.getInt();
        } catch (BufferUnderflowException ex) {
            throw new IOException("xdr stream too short");
        }
    }

    @Override
    public byte[] xdrDecodeOpaque(int length) throws OncRpcException, IOException {
        byte[] buf = new byte[length];
        xdrDecodeOpaque(buf, 0, length);
        return buf;
    }

    @Override
    public void xdrDecodeOpaque(byte[] opaque, int offset, int length) throws OncRpcException, IOException {
        int pad_len = length % 4 == 0 ? 0 : 4 - length % 4;

        while (length > 0) {
            if (!buf_.hasRemaining()) update_buffer_();
            final int rdlen = max(min(length, buf_.remaining()), 1);
            try {
                buf_.get(opaque, offset, rdlen);
            } catch (BufferUnderflowException ex) {
                throw new IOException("xdr stream too short");
            }
            offset += rdlen;
            length -= rdlen;
        }

        while (pad_len > 0) {
            if (!buf_.hasRemaining()) update_buffer_();
            try {
                buf_.get();
            } catch (BufferUnderflowException ex) {
                throw new IOException("xdr stream too short");
            }
            --pad_len;
        }
    }

    public long readBytes() { return read_bytes_ + buf_.position(); }
    public long avail() { return buf_.remaining(); }

    public boolean atEof() throws IOException {
        return !buf_.hasRemaining() && (buf_supplier_ == null || buf_supplier_.atEof());
    }
}
