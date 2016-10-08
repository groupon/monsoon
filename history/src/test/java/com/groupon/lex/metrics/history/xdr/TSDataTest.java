/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.history.TSData;
import com.groupon.lex.metrics.history.v2.list.FileListFileSupport;
import com.groupon.lex.metrics.history.v2.list.RWListFile;
import com.groupon.lex.metrics.history.v2.tables.FileTableFileSupport;
import com.groupon.lex.metrics.history.xdr.support.FileSupport;
import com.groupon.lex.metrics.history.xdr.support.FileSupport0;
import com.groupon.lex.metrics.history.xdr.support.FileSupport1;
import com.groupon.lex.metrics.history.xdr.support.FileTimeSeriesCollection;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Ignore;

/**
 *
 * @author ariane
 */
@RunWith(Parameterized.class)
public class TSDataTest {
    private final FileSupport file_support;
    private Path tmpdir, tmpfile;
    private final DateTime NOW = new DateTime(DateTimeZone.UTC);

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{ new FileSupport(new FileSupport0(), false) },
                new Object[]{ new FileSupport(new FileSupport1(), false) },
                new Object[]{ new FileSupport(new FileTableFileSupport(), false) },
                new Object[]{ new FileSupport(new FileListFileSupport(), false) },
                new Object[]{ new FileSupport(new FileSupport0(), true) },
                new Object[]{ new FileSupport(new FileSupport1(), true) },
                new Object[]{ new FileSupport(new FileTableFileSupport(), true) },
                new Object[]{ new FileSupport(new FileListFileSupport(), true) }
        );
    }

    public TSDataTest(FileSupport file_support) {
        this.file_support = file_support;
    }

    @Before
    public void setup() throws Exception {
        tmpdir = Files.createTempDirectory("monsoon-TSDataTest");
        tmpdir.toFile().deleteOnExit();
        tmpfile = tmpdir.resolve("test.tsd");
    }

    @After
    public void cleanup() throws Exception {
        Files.deleteIfExists(tmpfile);
    }

    @Test
    public void read() throws Exception {
        final List<TimeSeriesCollection> tsdata = create_tsdata_(100).collect(Collectors.toList());
        file_support.create_file(tmpfile, tsdata);

        final TSData fd = TSData.readonly(tmpfile);

        assertEquals(Files.size(tmpfile), fd.getFileSize());
        assertEquals(tsdata.get(0).getTimestamp(), fd.getBegin());
        assertEquals(tsdata.get(tsdata.size() - 1).getTimestamp(), fd.getEnd());
        assertNotNull(fd.getFileChannel());
        assertEquals(tsdata.size(), fd.size());
        assertEquals(file_support.getMajor(), fd.getMajor());
        assertEquals(file_support.getMinor(), fd.getMinor());
        assertThat(fd, IsIterableContainingInOrder.contains(tsdata.stream().map(Matchers::equalTo).collect(Collectors.toList())));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void cannot_add() throws Exception {
        final List<TimeSeriesCollection> tsdata = create_tsdata_(10).collect(Collectors.toList());
        file_support.create_file(tmpfile, tsdata);

        final TSData fd = TSData.readonly(tmpfile);
        fd.add(tsdata.get(0));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void cannot_add_all() throws Exception {
        final List<TimeSeriesCollection> tsdata = create_tsdata_(10).collect(Collectors.toList());
        file_support.create_file(tmpfile, tsdata);

        final TSData fd = TSData.readonly(tmpfile);
        fd.addAll(tsdata);
    }

    /* This test is designed not to fit in memory. */
    @Test
    @Ignore
    public void read_really_large_file() throws Exception {
        final int FILESIZE = 32 * 1024 * 1024;
        // Use the TSData writer to create this file.
        int count = 0;
        {
            File collectionFile = File.createTempFile("monsoon-", "-TSDataTest--really_large");
            collectionFile.deleteOnExit();
            final TSData writer = RWListFile.newFile(new GCCloseable<>(FileChannel.open(collectionFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)), true);
            Iterator<TimeSeriesCollection> iter = create_tsdata_(FILESIZE).iterator();

            while (writer.getFileSize() <= FILESIZE) {
                writer.add(iter.next());
                ++count;
            }

            file_support.create_file(tmpfile, writer);
        }

        // Use streaming matcher, to evade giant collection.
        final int expected_count = count;
        Iterator<TimeSeriesCollection> expected = create_tsdata_(FILESIZE).limit(expected_count).iterator();

        TSData fd = TSData.readonly(tmpfile);

        Iterator<TimeSeriesCollection> fd_iter = fd.iterator();
        while (expected.hasNext()) {
            assertTrue(fd_iter.hasNext());
            assertEquals(expected.next(), fd_iter.next());
        }
        assertFalse(fd_iter.hasNext());
    }

    private Stream<TimeSeriesCollection> create_tsdata_(int count) {
        final DateTime now = NOW.minusMinutes(count - 1);
        final Histogram hist = new Histogram(Stream.of(new Histogram.RangeWithCount(new Histogram.Range(0, 10), 10d), new Histogram.RangeWithCount(new Histogram.Range(100, 110), 10d)));

        return Stream.generate(
                new Supplier<Integer>(){
                    private int idx = 0;

                    @Override
                    public Integer get() {
                        return idx++;
                    }
                })
                .map(new Function<Integer, TimeSeriesCollection>() {
                    public FileTimeSeriesCollection apply(Integer i) {
                        return new FileTimeSeriesCollection(now.plusMinutes(i), Stream.of(
                                new MutableTimeSeriesValue(now.plusMinutes(i),
                                        GroupName.valueOf(SimpleGroupPath.valueOf("foo", "bar")),
                                        singletonMap(MetricName.valueOf("x"), MetricValue.fromIntValue(i))),
                                new MutableTimeSeriesValue(now.plusMinutes(i),
                                        GroupName.valueOf(SimpleGroupPath.valueOf("h", "i", "s", "t", "o", "g", "r", "a", "m")),
                                        singletonMap(MetricName.valueOf("x"), MetricValue.fromHistValue(hist)))
                                ));
                    }
                })
                .limit(count);
    }

    @Test
    public void to_array() throws Exception {
        final List<TimeSeriesCollection> tsdata = create_tsdata_(10).collect(Collectors.toList());
        file_support.create_file(tmpfile, tsdata);

        final TSData fd = TSData.readonly(tmpfile);

        assertArrayEquals(tsdata.toArray(), fd.toArray());
    }

    @Test
    public void to_array_with_array_constructor() throws Exception {
        final List<TimeSeriesCollection> tsdata = create_tsdata_(10).collect(Collectors.toList());
        file_support.create_file(tmpfile, tsdata);

        final TSData fd = TSData.readonly(tmpfile);

        assertArrayEquals(tsdata.toArray(new TimeSeriesCollection[0]), fd.toArray(new TimeSeriesCollection[0]));
    }

    @Test
    public void contains() throws Exception {
        final List<TimeSeriesCollection> tsdata = create_tsdata_(10).collect(Collectors.toList());
        file_support.create_file(tmpfile, tsdata.subList(0, 9));

        final TSData fd = TSData.readonly(tmpfile);

        assertTrue(fd.contains(tsdata.get(8)));
        assertFalse(fd.contains(tsdata.get(9)));
        assertFalse(fd.contains(new Object()));
    }

    @Test
    public void contains_all() throws Exception {
        final List<TimeSeriesCollection> tsdata = create_tsdata_(10).collect(Collectors.toList());
        file_support.create_file(tmpfile, tsdata.subList(0, 9));

        final TSData fd = TSData.readonly(tmpfile);

        assertTrue(fd.containsAll(tsdata.subList(1, 9)));
        assertFalse(fd.containsAll(tsdata));
        assertTrue(fd.containsAll(EMPTY_LIST));
        assertFalse(fd.containsAll(singletonList(new Object())));
    }

    @Test
    public void is_empty() throws Exception {
        file_support.create_file(tmpfile, EMPTY_LIST);

        final TSData fd = TSData.readonly(tmpfile);

        assertTrue(fd.isEmpty());
        assertEquals(0, fd.size());
    }

    @Test
    public void is_not_empty() throws Exception {
        final List<TimeSeriesCollection> tsdata = create_tsdata_(1).collect(Collectors.toList());
        file_support.create_file(tmpfile, tsdata);

        final TSData fd = TSData.readonly(tmpfile);

        assertFalse(fd.isEmpty());
        assertEquals(1, fd.size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void clear_is_not_supported() throws Exception {
        new TSDataMock().clear();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void retain_all_is_not_supported() throws Exception {
        new TSDataMock().retainAll(EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void remove_all_is_not_supported() throws Exception {
        new TSDataMock().removeAll(EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void remove_is_not_supported() throws Exception {
        new TSDataMock().remove(new Object());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void remove_if_is_not_supported() throws Exception {
        new TSDataMock().removeIf(x -> true);
    }

    @Test
    public void add_all_none_existing() {
        final List<TimeSeriesCollection> tsdata = create_tsdata_(10).collect(Collectors.toList());
        final Set<TimeSeriesCollection> result = new HashSet<>();
        final TSData impl = new TSDataMock() {
            @Override
            public boolean add(TimeSeriesCollection ts) { return result.add(ts); }
        };

        assertTrue(impl.addAll(tsdata));
    }

    @Test
    public void add_all_all_existing() {
        final List<TimeSeriesCollection> tsdata = create_tsdata_(10).collect(Collectors.toList());
        final Set<TimeSeriesCollection> result = new HashSet<>(tsdata);
        final TSData impl = new TSDataMock() {
            @Override
            public boolean add(TimeSeriesCollection ts) { return result.add(ts); }
        };

        assertFalse(impl.addAll(tsdata));
    }

    @Test
    public void add_all_some_existing() {
        final List<TimeSeriesCollection> tsdata = create_tsdata_(10).collect(Collectors.toList());
        final Set<TimeSeriesCollection> result = new HashSet<>(tsdata.subList(3, 6));
        final TSData impl = new TSDataMock() {
            @Override
            public boolean add(TimeSeriesCollection ts) { return result.add(ts); }
        };

        assertTrue(impl.addAll(tsdata));
    }

    private class TSDataMock implements TSData {
        @Override
        public DateTime getBegin() {
            return NOW;
        }

        @Override
        public DateTime getEnd() {
            return NOW;
        }

        @Override
        public long getFileSize() {
            fail("unimplemented mock function");
            return 0;
        }

        @Override
        public short getMajor() {
            return 0;
        }

        @Override
        public short getMinor() {
            return 0;
        }

        @Override
        public boolean canAddSingleRecord() {
            return false;
        }

        @Override
        public boolean isOptimized() {
            return false;
        }

        @Override
        public Iterator<TimeSeriesCollection> iterator() {
            fail("unimplemented mock function");
            return null;
        }

        @Override
        public Stream<TimeSeriesCollection> streamReversed() {
            fail("unimplemented mock function");
            return null;
        }

        @Override
        public boolean add(TimeSeriesCollection tsv) {
            fail("unimplemented mock function");
            return false;
        }
    }
}
