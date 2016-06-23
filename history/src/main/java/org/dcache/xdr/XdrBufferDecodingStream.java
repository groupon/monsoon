package org.dcache.xdr;

import com.groupon.lex.metrics.history.xdr.BufferSupplier;
import java.io.IOException;
import static java.lang.Math.max;
import static java.lang.Math.min;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import static java.util.Objects.requireNonNull;

/**
 *
 * @author ariane
 */
public class XdrBufferDecodingStream implements XdrDecodingStream {
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

    private void update_buffer_() throws BadXdrOncRpcException {
        if (buf_supplier_ == null) return;
        read_bytes_ += buf_.position();
        buf_.compact();
        try {
            buf_supplier_.load(buf_);
        } catch (IOException ex) {
            throw new BadXdrOncRpcException(ex.getMessage());
        }
        buf_.flip();
    }

    @Override
    public void beginDecoding() {}
    @Override
    public void endDecoding() {}

    @Override
    public int xdrDecodeInt() throws BadXdrOncRpcException {
        if (buf_.remaining() < 4) update_buffer_();
        try {
            return buf_.getInt();
        } catch (BufferUnderflowException ex) {
            throw new BadXdrOncRpcException("xdr stream too short");
        }
    }

    @Override
    public int[] xdrDecodeIntVector() throws BadXdrOncRpcException {
        int[] buf = new int[xdrDecodeInt()];
        for (int i = 0; i < buf.length; ++i)
            buf[i] = xdrDecodeInt();
        return buf;
    }

    @Override
    public byte[] xdrDecodeDynamicOpaque() throws BadXdrOncRpcException {
        return xdrDecodeOpaque(xdrDecodeInt());
    }

    @Override
    public byte[] xdrDecodeOpaque(int length) throws BadXdrOncRpcException {
        byte[] buf = new byte[length];
        xdrDecodeOpaque(buf, 0, length);
        return buf;
    }

    @Override
    public void xdrDecodeOpaque(byte[] opaque, int offset, int length) throws BadXdrOncRpcException {
        int pad_len = length % 4 == 0 ? 0 : 4 - length % 4;

        while (length > 0) {
            if (!buf_.hasRemaining()) update_buffer_();
            final int rdlen = max(min(length, buf_.remaining()), 1);
            try {
                buf_.get(opaque, offset, rdlen);
            } catch (BufferUnderflowException ex) {
                throw new BadXdrOncRpcException("xdr stream too short");
            }
            offset += rdlen;
            length -= rdlen;
        }

        while (pad_len > 0) {
            if (!buf_.hasRemaining()) update_buffer_();
            try {
                buf_.get();
            } catch (BufferUnderflowException ex) {
                throw new BadXdrOncRpcException("xdr stream too short");
            }
            --pad_len;
        }
    }

    @Override
    public boolean xdrDecodeBoolean() throws BadXdrOncRpcException {
        return xdrDecodeInt() != 0;
    }

    @Override
    public String xdrDecodeString() throws BadXdrOncRpcException {
        return new String(xdrDecodeOpaque(xdrDecodeInt()), Charset.forName("UTF-8"));
    }

    @Override
    public long xdrDecodeLong() throws BadXdrOncRpcException {
        if (buf_.remaining() < 8) update_buffer_();
        try {
            return buf_.getLong();
        } catch (BufferUnderflowException ex) {
            throw new BadXdrOncRpcException("xdr stream too short");
        }
    }

    @Override
    public long[] xdrDecodeLongVector() throws BadXdrOncRpcException {
        long[] buf = new long[xdrDecodeInt()];
        for (int i = 0; i < buf.length; ++i)
            buf[i] = xdrDecodeLong();
        return buf;
    }

    @Override
    public ByteBuffer xdrDecodeByteBuffer() throws BadXdrOncRpcException {
        final int len = xdrDecodeInt();

        if (buf_.capacity() >= len && buf_ instanceof ByteBuffer) {
            final int padding = (len % 4 == 0 ? 0 : 4 - len % 4);
            if (buf_.remaining() < len + padding) update_buffer_();
            if (buf_.remaining() >= len + padding) {
                ByteBuffer buf = ((ByteBuffer)buf_).slice();
                buf.limit(len);
                buf_.position(buf_.position() + len + padding);
                return buf;
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(len);
        xdrDecodeOpaque(buf.array(), buf.arrayOffset(), buf.arrayOffset() + buf.remaining());
        return buf;
    }

    @Override
    public float xdrDecodeFloat() throws BadXdrOncRpcException {
        return Float.intBitsToFloat(xdrDecodeInt());
    }

    @Override
    public double xdrDecodeDouble() throws BadXdrOncRpcException {
        return Double.longBitsToDouble(xdrDecodeLong());
    }

    @Override
    public double[] xdrDecodeDoubleVector() throws BadXdrOncRpcException {
        return xdrDecodeDoubleFixedVector(xdrDecodeInt());
    }

    @Override
    public double[] xdrDecodeDoubleFixedVector(int length) throws BadXdrOncRpcException {
        double[] buf = new double[length];
        for (int i = 0; i < buf.length; ++i)
            buf[i] = xdrDecodeDouble();
        return buf;
    }

    @Override
    public float[] xdrDecodeFloatVector() throws BadXdrOncRpcException {
        return xdrDecodeFloatFixedVector(xdrDecodeInt());
    }

    @Override
    public float[] xdrDecodeFloatFixedVector(int length) throws BadXdrOncRpcException {
        float[] buf = new float[length];
        for (int i = 0; i < buf.length; ++i)
            buf[i] = xdrDecodeFloat();
        return buf;
    }

    @Override
    public byte[] xdrDecodeByteVector() throws BadXdrOncRpcException {
        return xdrDecodeByteFixedVector(xdrDecodeInt());
    }

    @Override
    public byte[] xdrDecodeByteFixedVector(int length) throws BadXdrOncRpcException {
        byte[] buf = new byte[length];
        for (int i = 0; i < buf.length; ++i)
            buf[i] = xdrDecodeByte();
        return buf;
    }

    @Override
    public byte xdrDecodeByte() throws BadXdrOncRpcException {
        return (byte)xdrDecodeInt();
    }

    @Override
    public short xdrDecodeShort() throws BadXdrOncRpcException {
        return (short)xdrDecodeInt();
    }

    @Override
    public short[] xdrDecodeShortVector() throws BadXdrOncRpcException {
        return xdrDecodeShortFixedVector(xdrDecodeInt());
    }

    @Override
    public short[] xdrDecodeShortFixedVector(int length) throws BadXdrOncRpcException {
        short[] buf = new short[length];
        for (int i = 0; i < buf.length; ++i)
            buf[i] = xdrDecodeShort();
        return buf;
    }

    public long readBytes() { return read_bytes_ + buf_.position(); }
    public long avail() { return buf_.remaining(); }

    public boolean atEof() throws IOException {
        return !buf_.hasRemaining() && (buf_supplier_ == null || buf_supplier_.atEof());
    }
}
