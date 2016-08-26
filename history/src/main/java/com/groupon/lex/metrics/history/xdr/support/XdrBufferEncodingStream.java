package com.groupon.lex.metrics.history.xdr.support;

import java.io.IOException;
import static java.lang.Integer.min;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrEncodingStream;

public class XdrBufferEncodingStream extends XdrEncodingStream {
    public static int MIN_BUFSIZE = 1024;

    @Getter
    private final List<ByteBuffer> buffers = new ArrayList<>();

    public XdrBufferEncodingStream() {}
    public XdrBufferEncodingStream(int initSize) {
        this();
        buffers.add(newBuffer(initSize));
    }

    @Override
    public void beginEncoding(InetAddress receiverAddress, int receiverPort) throws OncRpcException, IOException {
        super.beginEncoding(receiverAddress, receiverPort);
    }
    public void beginEncoding() throws OncRpcException, IOException {
        beginEncoding(InetAddress.getLoopbackAddress(), 0);
    }

    @Override
    public void xdrEncodeInt(int value) throws OncRpcException, IOException {
        bufferForBytes(4).putInt(value);
    }

    @Override
    public void xdrEncodeOpaque(byte[] value, int offset, int length) throws OncRpcException, IOException {
        if (length < 0) throw new IllegalArgumentException("negative length");
        int pad_len = length % 4 == 0 ? 0 : 4 - length % 4;

        ByteBuffer lastBuf;
        if (buffers.isEmpty())
            lastBuf = bufferForBytes(length + pad_len);
        else
            lastBuf = buffers.get(buffers.size() - 1);

        if (lastBuf.hasRemaining()) {
            final int wlen = min(lastBuf.remaining(), length);
            lastBuf.put(value, offset, wlen);
            offset += wlen;
            length -= wlen;
        }
        if (length > 0) {
            lastBuf = bufferForBytes(length + pad_len);
            lastBuf.put(value, offset, length);
            offset += length;
            length = 0;
        }

        while (pad_len > 0) {
            if (!lastBuf.hasRemaining())
                lastBuf = bufferForBytes(pad_len);

            lastBuf.put((byte)0);
            --pad_len;
        }
    }

    private ByteBuffer bufferForBytes(int size) {
        if (buffers.isEmpty())
            buffers.add(newBuffer(size));

        ByteBuffer lastbuf = buffers.get(buffers.size() - 1);
        if (lastbuf.remaining() < size) {
            lastbuf = newBuffer(size);
            buffers.add(lastbuf);
        }
        return lastbuf;
    }

    private static ByteBuffer newBuffer(int size) {
        if (size < MIN_BUFSIZE) size = MIN_BUFSIZE;

        final ByteBuffer buf = ByteBuffer.allocateDirect(size);
        buf.order(ByteOrder.BIG_ENDIAN);
        return buf;
    }

    @Override
    public void endEncoding() throws OncRpcException, IOException {
        super.endEncoding();
        buffers.forEach(ByteBuffer::flip);
    }

    /**
     * Get the number of bytes in the buffers.
     * Value is undefined until endEncoding() has been called.
     *
     * @return the length (in bytes) of the buffers.
     */
    public int getBuffersLength() {
        return buffers.stream()
                .mapToInt(ByteBuffer::limit)
                .sum();
    }
}
