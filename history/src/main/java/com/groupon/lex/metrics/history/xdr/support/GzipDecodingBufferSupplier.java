package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.history.xdr.BufferSupplier;
import static com.groupon.lex.metrics.history.xdr.support.GzipHeaderConsts.ID1_EXPECT;
import static com.groupon.lex.metrics.history.xdr.support.GzipHeaderConsts.ID2_EXPECT;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import java.util.zip.ZipException;

/**
 *
 * @author ariane
 */
public class GzipDecodingBufferSupplier implements BufferSupplier {
    private static final Logger LOG = Logger.getLogger(GzipDecodingBufferSupplier.class.getName());
    private final static byte FLAG_HEADER_CRC = 2;
    private final static byte FLAG_EXTRA = 4;
    private final static byte FLAG_FNAME = 8;
    private final static byte FLAG_COMMENT = 16;
    private final CRC32 crc_ = new CRC32();
    private final Inflater inflater_ = new Inflater(true);
    private final BufferSupplier raw_;
    private final ByteBuffer raw_buf_;
    private boolean end_ = false;
    private Optional<String> fname_ = Optional.empty();
    private Optional<String> comment_ = Optional.empty();

    public GzipDecodingBufferSupplier(BufferSupplier raw) throws IOException {
        raw_buf_ = ByteBuffer.allocate(4096);
        raw_buf_.order(ByteOrder.LITTLE_ENDIAN);
        raw_ = requireNonNull(raw);
        raw_.load(raw_buf_);
        raw_buf_.flip();

        read_header_();
    }

    private void read_header_() throws IOException {
        crc_.reset();
        final byte id1 = read_byte_();
        final byte id2 = read_byte_();
        if (id1 != ID1_EXPECT || id2 != ID2_EXPECT)
            throw new ZipException("Not a valid GZIP stream");

        final byte compression_method = read_byte_();
        if (compression_method != 8)
            throw new ZipException("Unsupported compression method " + compression_method);
        final byte flags = read_byte_();
        skip_(6);  // Skip mtime, xfl, os.

        if ((flags & FLAG_EXTRA) == FLAG_EXTRA)
            skip_(read_short_());
        if ((flags & FLAG_FNAME) == FLAG_FNAME)
            fname_ = Optional.of(read_null_terminated_string_());
        else
            fname_ = Optional.empty();
        if ((flags & FLAG_COMMENT) == FLAG_COMMENT)
            comment_ = Optional.of(read_null_terminated_string_());
        else
            comment_ = Optional.empty();
        if ((flags & FLAG_HEADER_CRC) == FLAG_HEADER_CRC) {
            short read_crc = read_short_();
            short declared_crc = (short)(crc_.getValue() & 0xffff);
            LOG.log(Level.FINE, "read_crc = {0}, declared_crc = {1}", new Object[]{read_crc, declared_crc});
            if (read_crc != declared_crc)
                throw new ZipException("Invalid header CRC");
        }

        LOG.log(Level.FINE, "id1={0}, id2={1}, compression_method={2}, flags={3}, fname={4}, comment={5}", new Object[]{id1, id2, compression_method, Integer.toHexString(flags), fname_, comment_});

        /* Initial setup for decompression. */
        crc_.reset();
        inflater_.reset();
        if (!raw_buf_.hasRemaining()) raw_.load(raw_buf_);
        inflater_.setInput(raw_buf_.array(), raw_buf_.arrayOffset() + raw_buf_.position(), raw_buf_.remaining());
    }

    private boolean read_trailer_() throws IOException {
        final int crc_value = (int)crc_.getValue();
        final int declared_crc = read_int_();
        LOG.log(Level.FINE, "crc = {0}, expecting {1}", new Object[]{crc_value, declared_crc});
        if (declared_crc != crc_value)
            throw new ZipException("CRC mismatch");

        final int declared_fsize = read_int_();
        LOG.log(Level.FINE, "fsize (modulo int) = {0}, expecting {1}", new Object[]{(int)(inflater_.getBytesWritten() & 0xffffffff), declared_fsize});
        if ((int)(inflater_.getBytesWritten() & 0xffffffff) != declared_fsize)
            throw new ZipException("File size check mismatch");

        if (raw_.atEof()) return true;  // End-of-stream.
        // Not at end of stream, assume gzip concatenation.
        read_header_();
        return false;
    }

    private byte read_byte_() throws IOException {
        if (!raw_buf_.hasRemaining()) {
            raw_buf_.compact();
            raw_.load(raw_buf_);
            raw_buf_.flip();
        }
        final byte b = raw_buf_.get();
        crc_.update(new byte[]{ b });
        return b;
    }

    private short read_short_() throws IOException {
        if (raw_buf_.remaining() < 2) {
            raw_buf_.compact();
            raw_.load(raw_buf_);
            raw_buf_.flip();
        }
        final short s = raw_buf_.getShort();

        byte[] s_for_crc = new byte[2];
        s_for_crc[0] = raw_buf_.get(raw_buf_.position() - 2);
        s_for_crc[1] = raw_buf_.get(raw_buf_.position() - 1);
        crc_.update(s_for_crc);

        return s;
    }

    private int read_int_() throws IOException {
        if (raw_buf_.remaining() < 4) {
            raw_buf_.compact();
            raw_.load(raw_buf_);
            raw_buf_.flip();
        }
        final int s = raw_buf_.getInt();

        byte[] s_for_crc = new byte[4];
        s_for_crc[0] = raw_buf_.get(raw_buf_.position() - 4);
        s_for_crc[1] = raw_buf_.get(raw_buf_.position() - 3);
        s_for_crc[2] = raw_buf_.get(raw_buf_.position() - 2);
        s_for_crc[3] = raw_buf_.get(raw_buf_.position() - 1);
        crc_.update(s_for_crc);

        return s;
    }

    private void skip_(int bytes) throws IOException {
        for (;;) {
            if (raw_buf_.remaining() >= bytes) {
                assert(raw_buf_.hasArray());
                crc_.update(raw_buf_.array(), raw_buf_.arrayOffset() + raw_buf_.position(), bytes);
                raw_buf_.position(raw_buf_.position() + bytes);
                return;  // GUARD
            } else {
                assert(raw_buf_.hasArray());
                crc_.update(raw_buf_.array(), raw_buf_.arrayOffset() + raw_buf_.position(), raw_buf_.remaining());
                bytes -= raw_buf_.remaining();

                raw_buf_.clear();
                raw_.load(raw_buf_);
                raw_buf_.flip();
            }
        }
    }

    private String read_null_terminated_string_() throws IOException {
        /* Read until zero-byte. */
        final List<Byte> bytes_list = new ArrayList<>();
        for (byte b = read_byte_(); b != 0; b = read_byte_())
            bytes_list.add(b);
        /* Convert collection to array (java makes this surprisingly hard). */
        byte[] bytes = new byte[bytes_list.size()];
        for (int i = 0; i < bytes.length; ++i)
            bytes[i] = bytes_list.get(i);
        /* Convert byte array to string. */
        return new String(bytes, Charset.forName("UTF-8"));
    }

    @Override
    public void load(ByteBuffer buf) throws IOException {
        for (;;) {
            if (end_) return;

            if (inflater_.finished()) {
                // Sync input buffer.
                raw_buf_.position(raw_buf_.limit() - inflater_.getRemaining());

                LOG.log(Level.FINE, "reading trailer...");
                try {
                    end_ = read_trailer_();
                } catch (IOException ex) {
                    LOG.log(Level.FINE, "error reading trailer, closing file as a precaution...", ex);
                    end_ = true;
                    throw ex;
                }

                if (end_) return;
                continue;
            }

            if (inflater_.needsInput()) {
                raw_buf_.clear();
                raw_.load(raw_buf_);
                raw_buf_.flip();
                inflater_.setInput(raw_buf_.array(), raw_buf_.arrayOffset() + raw_buf_.position(), raw_buf_.remaining());
            }
            if (!buf.hasRemaining()) return;  // GUARD

            try {
                final int len;
                if (buf.hasArray()) {
                    len = inflater_.inflate(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
                    crc_.update(buf.array(), buf.arrayOffset() + buf.position(), len);
                    buf.position(buf.position() + len);
                } else {
                    byte[] tmp = new byte[buf.remaining()];
                    len = inflater_.inflate(tmp);
                    crc_.update(tmp, 0, len);
                    buf.put(tmp, 0, len);
                }
                LOG.log(Level.FINE, "inflated {0} bytes (total so far {1} bytes)", new Object[]{len, inflater_.getBytesWritten()});
            } catch (DataFormatException ex) {
                LOG.log(Level.WARNING, "unable to inflate", ex);
                throw new ZipException(Optional.ofNullable(ex.getMessage()).orElse("Invalid ZLIB data format"));
            }
        }
    }

    @Override
    public boolean atEof() {
        return end_;
    }

    public Optional<String> getFname() {
        return fname_;
    }

    public Optional<String> getComment() {
        return comment_;
    }
}
