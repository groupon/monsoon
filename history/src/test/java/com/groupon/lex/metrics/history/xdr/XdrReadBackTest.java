package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.history.v0.xdr.ToXdr;
import com.groupon.lex.metrics.history.v0.xdr.FromXdr;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.history.xdr.support.FileTimeSeriesCollection;
import com.groupon.lex.metrics.history.v0.xdr.tsfile_datapoint;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import static java.lang.Math.min;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.dcache.xdr.XdrBufferDecodingStream;
import org.dcache.xdr.XdrBufferEncodingStream;
import static org.junit.Assert.assertEquals;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class XdrReadBackTest {
    private static final SimpleGroupPath GROUP = SimpleGroupPath.valueOf("com", "test");
    private TimeSeriesCollection tsv_;

    private static byte[] to_byte_array(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private static byte[] to_byte_array(Iterable<ByteBuffer> buffers) {
        List<ByteBuffer> tmp = new ArrayList<>();
        buffers.forEach(tmp::add);

        int buflen = 0;
        for (ByteBuffer b : tmp) buflen += b.remaining();

        byte[] result = new byte[buflen];
        int off = 0;
        for (ByteBuffer b : tmp) {
            final int rdlen = b.remaining();
            b.get(result, off, rdlen);
            off += rdlen;
        }
        assert(off == result.length);
        return result;
    }

    private static class ByteArraySupplier implements BufferSupplier {
        private final byte[] data_;
        private int off_ = 0;

        public ByteArraySupplier(byte data[]) { data_ = data; }

        @Override
        public void load(ByteBuffer buf) {
            int rdlen = min(buf.remaining(), data_.length - off_);
            if (rdlen > 0) {
                buf.put(data_, off_, rdlen);
                off_ += rdlen;
            }
        }

        @Override
        public boolean atEof() {
            return off_ == data_.length;
        }
    }

    @Before
    public void setup() {
        final Map<MetricName, MetricValue> metrics = new HashMap<MetricName, MetricValue>() {{
            put(MetricName.valueOf("boolean"), MetricValue.fromBoolean(true));
            put(MetricName.valueOf("integer"), MetricValue.fromIntValue(17));
            put(MetricName.valueOf("float"), MetricValue.fromDblValue(Math.E));
            put(MetricName.valueOf("string"), MetricValue.fromStrValue("chocoladevla"));
        }};
        final DateTime now = DateTime.now(DateTimeZone.UTC);
        tsv_ = new FileTimeSeriesCollection(now, Stream.of(new MutableTimeSeriesValue(now, GroupName.valueOf(GROUP), metrics)));
    }

    @Test
    public void int_serialized() throws Exception {
        XdrBufferEncodingStream encoder = new XdrBufferEncodingStream(8);
        encoder.beginEncoding();
        encoder.xdrEncodeInt(17);
        encoder.endEncoding();
        byte[] result = to_byte_array(encoder.getBuffers());

        assertArrayEquals(new byte[]{ 0, 0, 0, 17 }, result);
    }

    @Test
    public void int_deserialized() throws Exception {
        XdrBufferDecodingStream decoder = new XdrBufferDecodingStream(new ByteArraySupplier(new byte[]{ 0, 0, 0, 19 }));
        int result = decoder.xdrDecodeInt();

        assertEquals(19, result);
        assertEquals(4L, decoder.readBytes());
    }

    @Test
    public void datapoint() throws Exception {
        tsfile_datapoint tsv_xdr = ToXdr.datapoints(tsv_);
        assertEquals(tsv_, FromXdr.datapoints(tsv_xdr));

        XdrBufferEncodingStream out = new XdrBufferEncodingStream(8);
        out.beginEncoding();
        tsv_xdr.xdrEncode(out);
        out.endEncoding();

        tsfile_datapoint tsv_xdr_read_back = new tsfile_datapoint();
        byte[] stream_bytes = to_byte_array(out.getBuffers());
        XdrBufferDecodingStream stream = new XdrBufferDecodingStream(new ByteArraySupplier(stream_bytes));
        tsv_xdr_read_back.xdrDecode(stream);

        TimeSeriesCollection read_tsv = FromXdr.datapoints(tsv_xdr_read_back);
        assertEquals(tsv_, read_tsv);
        assertEquals(stream_bytes.length, stream.readBytes());
    }

    @Test(expected = IOException.class)
    public void datapoint_read_too_many() throws Exception {
        tsfile_datapoint tsv_xdr = ToXdr.datapoints(tsv_);
        XdrBufferEncodingStream out = new XdrBufferEncodingStream(512);
        out.beginEncoding();
        tsv_xdr.xdrEncode(out);  // Write 1 entry
        out.endEncoding();

        tsfile_datapoint tsv_xdr_read_back = new tsfile_datapoint();
        XdrBufferDecodingStream stream = new XdrBufferDecodingStream(new ByteArraySupplier(to_byte_array(out.getBuffers())));

        tsv_xdr_read_back.xdrDecode(stream);  // Read 1 entry
        TimeSeriesCollection read_tsv = FromXdr.datapoints(tsv_xdr_read_back);
        assertEquals(tsv_, read_tsv);

        tsv_xdr_read_back.xdrDecode(stream);  // Read the second, non-existant entry (i.e. this should fail)
        TimeSeriesCollection read_tsv_too_many = FromXdr.datapoints(tsv_xdr_read_back);
        fail("unreachable: expected an exception here");
    }

    @Test(expected = IOException.class)
    public void not_too_many_bytes_written() throws Exception {
        tsfile_datapoint tsv_xdr = ToXdr.datapoints(tsv_);
        XdrBufferEncodingStream out = new XdrBufferEncodingStream(4);
        out.beginEncoding();
        tsv_xdr.xdrEncode(out);  // Write 1 entry
        out.endEncoding();

        tsfile_datapoint tsv_xdr_read_back = new tsfile_datapoint();
        XdrBufferDecodingStream stream = new XdrBufferDecodingStream(new ByteArraySupplier(to_byte_array(out.getBuffers())));

        tsv_xdr_read_back.xdrDecode(stream);  // Read 1 entry
        TimeSeriesCollection read_tsv = FromXdr.datapoints(tsv_xdr_read_back);
        assertEquals(tsv_, read_tsv);

        /*
         * Attempts to read more data should fail.
         */
        assertEquals(0L, stream.avail());
        stream.xdrDecodeByte();  // Must throw
    }

    @Test
    public void read_multiple() throws Exception {
        final int N = 1000000;

        tsfile_datapoint tsv_xdr = ToXdr.datapoints(tsv_);
        XdrBufferEncodingStream out = new XdrBufferEncodingStream(8);
        out.beginEncoding();
        for (int i = 0; i < N; ++i)
            tsv_xdr.xdrEncode(out);  // Write 1 entry
        out.endEncoding();

        tsfile_datapoint tsv_xdr_read_back = new tsfile_datapoint();
        byte[] stream_bytes = to_byte_array(out.getBuffers());
        XdrBufferDecodingStream stream = new XdrBufferDecodingStream(new ByteArraySupplier(stream_bytes));

        for (int i = 0; i < N; ++i) {
            tsv_xdr_read_back.xdrDecode(stream);  // Read 1 entry
            TimeSeriesCollection read_tsv = FromXdr.datapoints(tsv_xdr_read_back);
            assertEquals(tsv_, read_tsv);
        }

        assertEquals(stream_bytes.length, stream.readBytes());
        assertEquals(0L, stream.avail());
    }
}
