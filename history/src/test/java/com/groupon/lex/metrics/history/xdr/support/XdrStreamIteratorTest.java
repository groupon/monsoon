package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.history.xdr.BufferSupplier;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.READ;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 * @author ariane
 */
@RunWith(Parameterized.class)
public class XdrStreamIteratorTest {
    private final FileSupport file_support;
    private Path tmpdir, tmpfile;
    private List<FileTimeSeriesCollection> tsdata;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{ new FileSupport((short)0, (short)1) },
                new Object[]{ new FileSupport((short)1, (short)0) }
        );
    }

    public XdrStreamIteratorTest(FileSupport file_support) {
        this.file_support = file_support;
    }

    @Before
    public void setup() throws Exception {
        final DateTime now = new DateTime(DateTimeZone.UTC);

        tsdata = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            tsdata.add(new FileTimeSeriesCollection(now.plusMinutes(i), Stream.of(
                new MutableTimeSeriesValue(now.plusMinutes(i),
                        new GroupName(new SimpleGroupPath("foo", "bar")),
                        singletonMap(new MetricName("x"), MetricValue.fromIntValue(i))))));
        }

        tmpdir = Files.createTempDirectory("monsoon-XdrStreamIteratorTest");
        tmpfile = tmpdir.resolve("test.tsv");
        file_support.create_file(tmpfile, tsdata);
    }

    @After
    public void cleanup() throws Exception {
        Files.deleteIfExists(tmpfile);
        Files.deleteIfExists(tmpdir);
    }

    @Test
    public void iterator_using_supplier() throws Exception {
        try (FileChannel fd = FileChannel.open(tmpfile, READ)) {
            XdrStreamIterator iter = new XdrStreamIterator(new BufferSupplier() {
                @Override
                public void load(ByteBuffer buf) throws IOException {
                    fd.read(buf);
                }

                @Override
                public boolean atEof() throws IOException {
                    return fd.position() == fd.size();
                }
            });

            assertTrue(fd.position() > 0);  // Must have validated buffer.
            assertTrue(iter.hasNext());

            /* Read all the things. */
            List<TimeSeriesCollection> read_tsdata = new ArrayList<>();
            iter.forEachRemaining(read_tsdata::add);

            assertTrue(fd.position() == fd.size());  // At end-of-file.
            assertEquals(tsdata, read_tsdata);
        }
    }

    @Test
    public void iterator_using_bytebuffer() throws Exception {
        final MappedByteBuffer buf;
        try (FileChannel fd = FileChannel.open(tmpfile, READ)) {
            buf = fd.map(FileChannel.MapMode.READ_ONLY, 0, fd.size());
        }

        XdrStreamIterator iter = new XdrStreamIterator(buf);

        assertTrue(buf.position() > 0);  // Must have validated buffer.
        assertTrue(iter.hasNext());

        /* Read all the things. */
        List<TimeSeriesCollection> read_tsdata = new ArrayList<>();
        iter.forEachRemaining(read_tsdata::add);

        assertFalse(buf.hasRemaining());  // At end-of-file.
        assertEquals(tsdata, read_tsdata);
    }

    @Test(expected = NoSuchElementException.class)
    public void no_such_element_exception() throws Exception {
        final MappedByteBuffer buf;
        try (FileChannel fd = FileChannel.open(tmpfile, READ)) {
            buf = fd.map(FileChannel.MapMode.READ_ONLY, 0, fd.size());
        }

        XdrStreamIterator iter = new XdrStreamIterator(buf);

        assertTrue(buf.position() > 0);  // Must have validated buffer.
        assertTrue(iter.hasNext());

        /* Read all the things. */
        List<TimeSeriesCollection> read_tsdata = new ArrayList<>();
        iter.forEachRemaining(read_tsdata::add);

        assertFalse(buf.hasRemaining());  // At end-of-file.
        assertFalse(iter.hasNext());
        iter.next();
    }

    @Test(expected = RuntimeException.class)
    public void truncated_file() throws Exception {
        final MappedByteBuffer buf;
        try (FileChannel fd = FileChannel.open(tmpfile, READ)) {
            buf = fd.map(FileChannel.MapMode.READ_ONLY, 0, fd.size() - 10);
        }

        XdrStreamIterator iter = new XdrStreamIterator(buf);

        assertTrue(buf.position() > 0);  // Must have validated buffer.
        assertTrue(iter.hasNext());

        /* Read all the things. */
        List<TimeSeriesCollection> read_tsdata = new ArrayList<>();
        iter.forEachRemaining(read_tsdata::add);
    }
}
