/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dcache.xdr;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
public class XdrBufferDecodingStreamTest {
    private ByteBuffer direct;
    private ByteBuffer array;

    @Before
    public void setup() {
        direct = ByteBuffer.allocateDirect(8192).order(ByteOrder.BIG_ENDIAN);
        array  = ByteBuffer.allocate(8192).order(ByteOrder.BIG_ENDIAN);
    }

    @Test
    public void read_long() throws Exception {
        direct.putLong(17).flip();
        array.putLong(19).flip();

        assertEquals(17, new XdrBufferDecodingStream(direct).xdrDecodeLong());
        assertEquals(19, new XdrBufferDecodingStream(array).xdrDecodeLong());
        assertEquals(8, direct.position());
        assertEquals(8, array.position());
    }

    @Test
    public void read_int() throws Exception {
        direct.putInt(17).flip();
        array.putInt(19).flip();

        assertEquals(17, new XdrBufferDecodingStream(direct).xdrDecodeInt());
        assertEquals(19, new XdrBufferDecodingStream(array).xdrDecodeInt());
        assertEquals(4, direct.position());
        assertEquals(4, array.position());
    }

    @Test
    public void read_short() throws Exception {
        direct.putInt(17).flip();
        array.putInt(19).flip();

        assertEquals(17, new XdrBufferDecodingStream(direct).xdrDecodeShort());
        assertEquals(19, new XdrBufferDecodingStream(array).xdrDecodeShort());
        assertEquals(4, direct.position());
        assertEquals(4, array.position());
    }

    @Test
    public void read_byte() throws Exception {
        direct.putInt(17).flip();
        array.putInt(19).flip();

        assertEquals(17, new XdrBufferDecodingStream(direct).xdrDecodeByte());
        assertEquals(19, new XdrBufferDecodingStream(array).xdrDecodeByte());
        assertEquals(4, direct.position());
        assertEquals(4, array.position());
    }

    @Test
    public void read_float() throws Exception {
        direct.putInt(Float.floatToIntBits(17e4f)).flip();
        array.putInt(Float.floatToIntBits(19e4f)).flip();

        assertEquals(17e4f, new XdrBufferDecodingStream(direct).xdrDecodeFloat(), 0.01);
        assertEquals(19e4f, new XdrBufferDecodingStream(array).xdrDecodeFloat(), 0.01);
        assertEquals(4, direct.position());
        assertEquals(4, array.position());
    }

    @Test
    public void read_double() throws Exception {
        direct.putLong(Double.doubleToLongBits(17e4)).flip();
        array.putLong(Double.doubleToLongBits(19e4)).flip();

        assertEquals(17e4, new XdrBufferDecodingStream(direct).xdrDecodeDouble(), 0.01);
        assertEquals(19e4, new XdrBufferDecodingStream(array).xdrDecodeDouble(), 0.01);
        assertEquals(8, direct.position());
        assertEquals(8, array.position());
    }

    @Test
    public void read_string() throws Exception {
        direct.putInt(7).put("Monsoon\0".getBytes()).flip();
        array.putInt(7).put("Monsoon\0".getBytes()).flip();

        assertEquals("Monsoon", new XdrBufferDecodingStream(direct).xdrDecodeString());
        assertEquals("Monsoon", new XdrBufferDecodingStream(array).xdrDecodeString());
        assertEquals(12, direct.position());
        assertEquals(12, array.position());
    }

    @Test
    public void read_byte_vector() throws Exception {
        direct.putInt(2).putInt(17).putInt(19).flip();
        array.putInt(2).putInt(17).putInt(19).flip();

        assertArrayEquals(new byte[]{ 17, 19 }, new XdrBufferDecodingStream(direct).xdrDecodeByteVector());
        assertArrayEquals(new byte[]{ 17, 19 }, new XdrBufferDecodingStream(array).xdrDecodeByteVector());
    }

    @Test
    public void read_short_vector() throws Exception {
        direct.putInt(2).putInt(17).putInt(19).flip();
        array.putInt(2).putInt(17).putInt(19).flip();

        assertArrayEquals(new short[]{ 17, 19 }, new XdrBufferDecodingStream(direct).xdrDecodeShortVector());
        assertArrayEquals(new short[]{ 17, 19 }, new XdrBufferDecodingStream(array).xdrDecodeShortVector());
    }

    @Test
    public void read_int_vector() throws Exception {
        direct.putInt(2).putInt(17).putInt(19).flip();
        array.putInt(2).putInt(17).putInt(19).flip();

        assertArrayEquals(new int[]{ 17, 19 }, new XdrBufferDecodingStream(direct).xdrDecodeIntVector());
        assertArrayEquals(new int[]{ 17, 19 }, new XdrBufferDecodingStream(array).xdrDecodeIntVector());
    }

    @Test
    public void read_long_vector() throws Exception {
        direct.putInt(2).putLong(17).putLong(19).flip();
        array.putInt(2).putLong(17).putLong(19).flip();

        assertArrayEquals(new long[]{ 17, 19 }, new XdrBufferDecodingStream(direct).xdrDecodeLongVector());
        assertArrayEquals(new long[]{ 17, 19 }, new XdrBufferDecodingStream(array).xdrDecodeLongVector());
    }

    @Test
    public void read_float_vector() throws Exception {
        direct.putInt(2).putInt(Float.floatToIntBits(17)).putInt(Float.floatToIntBits(19)).flip();
        array.putInt(2).putInt(Float.floatToIntBits(17)).putInt(Float.floatToIntBits(19)).flip();

        float[] direct_decoded = new XdrBufferDecodingStream(direct).xdrDecodeFloatVector();
        assertEquals(2, direct_decoded.length);
        assertEquals(17f, direct_decoded[0], 0.01);
        assertEquals(19f, direct_decoded[1], 0.01);
        float[] array_decoded = new XdrBufferDecodingStream(array).xdrDecodeFloatVector();
        assertEquals(2, array_decoded.length);
        assertEquals(17f, array_decoded[0], 0.01);
        assertEquals(19f, array_decoded[1], 0.01);
    }

    @Test
    public void read_double_vector() throws Exception {
        direct.putInt(2).putLong(Double.doubleToLongBits(17)).putLong(Double.doubleToLongBits(19)).flip();
        array.putInt(2).putLong(Double.doubleToLongBits(17)).putLong(Double.doubleToLongBits(19)).flip();

        double[] direct_decoded = new XdrBufferDecodingStream(direct).xdrDecodeDoubleVector();
        assertEquals(2, direct_decoded.length);
        assertEquals(17d, direct_decoded[0], 0.01);
        assertEquals(19d, direct_decoded[1], 0.01);
        double[] array_decoded = new XdrBufferDecodingStream(array).xdrDecodeDoubleVector();
        assertEquals(2, array_decoded.length);
        assertEquals(17d, array_decoded[0], 0.01);
        assertEquals(19d, array_decoded[1], 0.01);
    }

    @Test
    public void read_opaque_into_array() throws Exception {
        direct.put((byte)8).put((byte)9).put((byte)10).put((byte)11).flip();
        array.put((byte)8).put((byte)9).put((byte)10).put((byte)11).flip();

        byte[] direct_buf = new byte[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        new XdrBufferDecodingStream(direct).xdrDecodeOpaque(direct_buf, 3, 3);
        byte[] array_buf = new byte[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        new XdrBufferDecodingStream(array).xdrDecodeOpaque(array_buf, 3, 3);

        assertArrayEquals(new byte[]{ 0, 1, 2, 8, 9, 10, 6, 7, 8, 9 }, direct_buf);
        assertArrayEquals(new byte[]{ 0, 1, 2, 8, 9, 10, 6, 7, 8, 9 }, array_buf);
    }

    @Test
    public void read_opaque() throws Exception {
        direct.put((byte)8).put((byte)9).put((byte)10).put((byte)11).flip();
        array.put((byte)8).put((byte)9).put((byte)10).put((byte)11).flip();

        byte[] direct_buf = new XdrBufferDecodingStream(direct).xdrDecodeOpaque(3);
        byte[] array_buf = new XdrBufferDecodingStream(array).xdrDecodeOpaque(3);

        assertArrayEquals(new byte[]{ 8, 9, 10 }, direct_buf);
        assertArrayEquals(new byte[]{ 8, 9, 10 }, array_buf);
    }

    @Test
    public void read_dynamic_opaque() throws Exception {
        direct.putInt(3).put((byte)8).put((byte)9).put((byte)10).put((byte)11).flip();
        array.putInt(3).put((byte)8).put((byte)9).put((byte)10).put((byte)11).flip();

        byte[] direct_buf = new XdrBufferDecodingStream(direct).xdrDecodeDynamicOpaque();
        byte[] array_buf = new XdrBufferDecodingStream(array).xdrDecodeDynamicOpaque();

        assertArrayEquals(new byte[]{ 8, 9, 10 }, direct_buf);
        assertArrayEquals(new byte[]{ 8, 9, 10 }, array_buf);
    }

    @Test
    public void read_byte_buffer() throws Exception {
        direct.putInt(3).put((byte)8).put((byte)9).put((byte)10).put((byte)11).flip();
        array.putInt(3).put((byte)8).put((byte)9).put((byte)10).put((byte)11).flip();

        byte[] direct_buf = new byte[3];
        boolean direct_remaining = new XdrBufferDecodingStream(direct).xdrDecodeByteBuffer().get(direct_buf).hasRemaining();
        byte[] array_buf = new byte[3];
        boolean array_remaining = new XdrBufferDecodingStream(array).xdrDecodeByteBuffer().get(array_buf).hasRemaining();

        assertFalse(direct_remaining);
        assertArrayEquals(new byte[]{ 8, 9, 10 }, direct_buf);
        assertFalse(array_remaining);
        assertArrayEquals(new byte[]{ 8, 9, 10 }, array_buf);
    }

    @Test
    public void at_eof() throws Exception {
        direct.putLong(17).flip();
        XdrBufferDecodingStream direct_decoder = new XdrBufferDecodingStream(direct);
        array.putLong(19).flip();
        XdrBufferDecodingStream array_decoder = new XdrBufferDecodingStream(array);

        assertFalse(direct_decoder.atEof());
        direct_decoder.xdrDecodeLong();
        assertTrue(direct_decoder.atEof());

        assertFalse(array_decoder.atEof());
        array_decoder.xdrDecodeLong();
        assertTrue(array_decoder.atEof());
    }
}
