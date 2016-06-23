package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.history.xdr.BufferSupplier;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import static java.util.Arrays.copyOfRange;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.Random;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class GzipDecodingBufferSupplierTest {
    public final static int REPEAT = 3;
    private byte[] plaintext;
    private byte[] compressed_data;
    private byte[] zero_byte_compressed;
    private byte[] with_extra_data;
    private byte[] with_comment;

    /** An empty file, compressed with GZIP, at level 9. Filename is zerobytes.txt */
    private static final byte[] ZERO_BYTE_COMPRESSED = new byte[]{
            /* 0000000 */ to_byte_(0x1f), to_byte_(0x8b), to_byte_(0x08), to_byte_(0x08), to_byte_(0x17), to_byte_(0x7b), to_byte_(0x30), to_byte_(0x57),
            /* 0000008 */ to_byte_(0x02), to_byte_(0x03), to_byte_(0x7a), to_byte_(0x65), to_byte_(0x72), to_byte_(0x6f), to_byte_(0x62), to_byte_(0x79),
            /* 0000010 */ to_byte_(0x74), to_byte_(0x65), to_byte_(0x73), to_byte_(0x2e), to_byte_(0x74), to_byte_(0x78), to_byte_(0x74), to_byte_(0x00),
            /* 0000018 */ to_byte_(0x03), to_byte_(0x00), to_byte_(0x00), to_byte_(0x00), to_byte_(0x00), to_byte_(0x00), to_byte_(0x00), to_byte_(0x00),
            /* 0000020 */ to_byte_(0x00), to_byte_(0x00)
            };

    private static class ByteArrayBufferSupplier implements BufferSupplier {
        private int repeat_;
        private final byte[] data_;
        private int offset_ = 0;

        public ByteArrayBufferSupplier(byte[] data, int repeat) {
            data_ = requireNonNull(data);
            repeat_ = repeat;
        }

        public ByteArrayBufferSupplier(byte[] data) {
            this(data, 1);
        }

        @Override
        public void load(ByteBuffer buf) {
            while (buf.hasRemaining() && !atEof()) {
                final int rlen = Integer.min(buf.remaining(), data_.length - offset_);
                buf.put(data_, offset_, rlen);
                offset_ += rlen;

                if (offset_ == data_.length) {
                    offset_ = 0;
                    --repeat_;
                }
            }
        }

        @Override
        public boolean atEof() {
            return repeat_ == 0 && offset_ == 0;
        }
    }

    @Before
    public void setup() throws Exception {
        zero_byte_compressed = Arrays.copyOf(ZERO_BYTE_COMPRESSED, ZERO_BYTE_COMPRESSED.length);
        plaintext = new byte[8 * 1024 * 1024];
        new Random().nextBytes(plaintext);

        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (OutputStream out = new GZIPOutputStream(buffer)) {
            out.write(plaintext);
        }

        compressed_data = buffer.toByteArray();

        /*
         * We're programmatically mangling the zero-byte file, to add the 'extra' field.
         */
        with_extra_data = Arrays.copyOf(ZERO_BYTE_COMPRESSED, ZERO_BYTE_COMPRESSED.length + 4);
        // Make space for the 'extra' data, starting at the byte 10.
        for (int i = 10; i < zero_byte_compressed.length; ++i)
            with_extra_data[i] = zero_byte_compressed[i];
        // Specify in the flags that the extra data is present.
        with_extra_data[3] |= 0x04;
        // Specify extra length of 2 bytes.
        with_extra_data[10] = 0x02;
        with_extra_data[11] = 0x00;
        // Add extra data of 2 bytes: 23, 29.
        with_extra_data[12] = 23;
        with_extra_data[13] = 29;

        /*
         * We're programmatically mangling the zero-byte file, to remove the filename and instead add a comment.
         * The comment is 'zerobytes.txt', since we're making use of the fact that if only one of them is present, they occupy the same position.
         */
        with_comment = Arrays.copyOf(ZERO_BYTE_COMPRESSED, ZERO_BYTE_COMPRESSED.length);
        with_comment[3] |= 0x10;  // Enable comment flag.
        with_comment[3] &= ~(byte)0x08;  // Clear filename flag.
    }

    @Test
    public void read_using_alloc_buffer() throws Exception {
        final BufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(compressed_data));
        final ByteBuffer buf = ByteBuffer.allocate(512);
        final ByteArrayOutputStream decoded_data = new ByteArrayOutputStream(plaintext.length);

        while (!decoder.atEof()) {
            decoder.load(buf);
            assertTrue("decoder must entirely fill buffer", decoder.atEof() || !buf.hasRemaining());
            buf.flip();
            decoded_data.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
            buf.clear();  // Since we read all data in the buffer, we might as well clear it, instead of advancing the position + compacting.
        }
        decoded_data.close();

        assertArrayEquals(plaintext, decoded_data.toByteArray());
    }

    @Test
    public void read_using_direct_buffer() throws Exception {
        final BufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(compressed_data));
        final ByteBuffer buf = ByteBuffer.allocateDirect(512);
        final ByteArrayOutputStream decoded_data = new ByteArrayOutputStream(plaintext.length);

        while (!decoder.atEof()) {
            decoder.load(buf);
            assertTrue("decoder must entirely fill buffer", decoder.atEof() || !buf.hasRemaining());
            buf.flip();
            {
                byte[] tmpbuf = new byte[buf.remaining()];
                buf.get(tmpbuf);
                decoded_data.write(tmpbuf);
            }
            buf.compact();
        }
        decoded_data.close();

        assertArrayEquals(plaintext, decoded_data.toByteArray());
    }

    @Test
    public void read_repeated_gzip_using_alloc_buffer() throws Exception {
        final BufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(compressed_data, REPEAT));
        final ByteBuffer buf = ByteBuffer.allocate(512);
        final ByteArrayOutputStream decoded_data = new ByteArrayOutputStream(REPEAT * plaintext.length);

        while (!decoder.atEof()) {
            decoder.load(buf);
            assertTrue("decoder must entirely fill buffer", decoder.atEof() || !buf.hasRemaining());
            buf.flip();
            decoded_data.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
            buf.clear();  // Since we read all data in the buffer, we might as well clear it, instead of advancing the position + compacting.
        }
        decoded_data.close();

        assertEquals(REPEAT * plaintext.length, decoded_data.toByteArray().length);
        for (int i = 0; i < REPEAT; ++i) {
            final byte[] slice = copyOfRange(decoded_data.toByteArray(), i * plaintext.length, (i + 1) * plaintext.length);
            assertArrayEquals(plaintext, slice);
        }
    }

    @Test
    public void read_repeated_gzip_using_direct_buffer() throws Exception {
        final BufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(compressed_data, REPEAT));
        final ByteBuffer buf = ByteBuffer.allocateDirect(512);
        final ByteArrayOutputStream decoded_data = new ByteArrayOutputStream(REPEAT * plaintext.length);

        while (!decoder.atEof()) {
            decoder.load(buf);
            assertTrue("decoder must entirely fill buffer", decoder.atEof() || !buf.hasRemaining());
            buf.flip();
            {
                byte[] tmpbuf = new byte[buf.remaining()];
                buf.get(tmpbuf);
                decoded_data.write(tmpbuf);
            }
            buf.compact();  // Since we read all data in the buffer, we might as well clear it, instead of advancing the position + compacting.
        }
        decoded_data.close();

        assertEquals(REPEAT * plaintext.length, decoded_data.toByteArray().length);
        for (int i = 0; i < REPEAT; ++i) {
            final byte[] slice = copyOfRange(decoded_data.toByteArray(), i * plaintext.length, (i + 1) * plaintext.length);
            assertArrayEquals(plaintext, slice);
        }
    }

    @Test
    public void read_alt_code_paths() throws Exception {
        final GzipDecodingBufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(zero_byte_compressed));
        assertEquals(Optional.of("zerobytes.txt"), decoder.getFname());
        assertEquals(Optional.empty(), decoder.getComment());
        final ByteBuffer buf = ByteBuffer.allocate(32);

        decoder.load(buf);
        buf.flip();

        assertTrue(decoder.atEof());
        assertFalse(buf.hasRemaining());
    }

    @Test(expected = ZipException.class)
    public void corrupt_file_len() throws Exception {
        zero_byte_compressed[zero_byte_compressed.length - 4] = 0x01;
        final GzipDecodingBufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(zero_byte_compressed));
        final ByteBuffer buf = ByteBuffer.allocate(32);
        decoder.load(buf);
    }

    @Test(expected = ZipException.class)
    public void corrupt_file_crc() throws Exception {
        zero_byte_compressed[zero_byte_compressed.length - 8] = 0x01;
        final GzipDecodingBufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(zero_byte_compressed));
        final ByteBuffer buf = ByteBuffer.allocate(32);
        decoder.load(buf);
    }

    @Test(expected = ZipException.class)
    public void unrecognized_compression() throws Exception {
        zero_byte_compressed[2] = 0x01;  // Third byte indicates the compression method; deflate = 0x08.
        final GzipDecodingBufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(zero_byte_compressed));
        final ByteBuffer buf = ByteBuffer.allocate(32);
        decoder.load(buf);
    }

    @Test(expected = ZipException.class)
    public void not_a_zip_file_1() throws Exception {
        zero_byte_compressed[0] = 0x01;
        final GzipDecodingBufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(zero_byte_compressed));
        final ByteBuffer buf = ByteBuffer.allocate(32);
        decoder.load(buf);
    }

    @Test(expected = ZipException.class)
    public void not_a_zip_file_2() throws Exception {
        zero_byte_compressed[1] = 0x01;
        final GzipDecodingBufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(zero_byte_compressed));
        final ByteBuffer buf = ByteBuffer.allocate(32);
        decoder.load(buf);
    }

    @Test(expected = ZipException.class)
    public void not_a_zip_file_3() throws Exception {
        zero_byte_compressed[0] = 0x01;
        zero_byte_compressed[1] = 0x01;
        final GzipDecodingBufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(zero_byte_compressed));
        final ByteBuffer buf = ByteBuffer.allocate(32);
        decoder.load(buf);
    }

    /** Hit a byte underflow. */
    @Test(expected = BufferUnderflowException.class)
    public void too_short_2_byte() throws Exception {
        final GzipDecodingBufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(Arrays.copyOf(zero_byte_compressed, 2)));
        final ByteBuffer buf = ByteBuffer.allocate(32);
        decoder.load(buf);
    }

    /** Hit the short for extra-data. */
    @Test(expected = BufferUnderflowException.class)
    public void too_short_11_byte() throws Exception {
        final GzipDecodingBufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(Arrays.copyOf(with_extra_data, 11)));
        final ByteBuffer buf = ByteBuffer.allocate(32);
        decoder.load(buf);
    }

    @Test
    public void read_with_extra_data() throws Exception {
        final GzipDecodingBufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(with_extra_data));
        final ByteBuffer buf = ByteBuffer.allocate(32);
        decoder.load(buf);
        buf.flip();

        assertTrue(decoder.atEof());
        assertFalse(buf.hasRemaining());
    }

    @Test
    public void read_comment() throws Exception {
        final GzipDecodingBufferSupplier decoder = new GzipDecodingBufferSupplier(new ByteArrayBufferSupplier(with_comment));
        assertEquals(Optional.empty(), decoder.getFname());
        assertEquals(Optional.of("zerobytes.txt"), decoder.getComment());
        final ByteBuffer buf = ByteBuffer.allocate(32);

        decoder.load(buf);
        buf.flip();

        assertTrue(decoder.atEof());
        assertFalse(buf.hasRemaining());
    }

    /** Create bytes from integers, since Java is cumbersome in declaring byte constants. */
    private static byte to_byte_(int b) {
        assert(b >= 0 && b < 256);
        return (byte)b;
    }
}
