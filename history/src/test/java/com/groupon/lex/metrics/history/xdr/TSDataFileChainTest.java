/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.history.TSData;
import com.groupon.lex.metrics.history.xdr.support.FileSupport;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.reverse;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class TSDataFileChainTest {
    private static final Logger LOG = Logger.getLogger(TSDataFileChainTest.class.getName());
    private final FileSupport file_support = new FileSupport(Const.MAJOR, Const.MINOR);
    private Path tmpdir, emptydir;
    private TSDataFileChain fd;
    public static final int LARGE_COUNT = 250;

    @Before
    public void setup() throws Exception {
        tmpdir = Files.createTempDirectory("monsoon-TSDataFileChainTest");
        tmpdir.toFile().deleteOnExit();
        emptydir = Files.createTempDirectory("monsoon-TSDataFileChainTest-empty");
        emptydir.toFile().deleteOnExit();
        fd = TSDataFileChain.openDir(tmpdir, 2 * 1024 * 1024);
    }

    @After
    public void cleanup() throws Exception {
        Files.list(tmpdir).forEach((f) -> {
            try {
                Files.delete(f);
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "unable to delete " + f, ex);
            }
        });

        fd = null;
    }

    @Test
    public void versioning() {
        fill_(1);
        assertEquals(Const.MAJOR, fd.getMajor());
        assertEquals(Const.MINOR, fd.getMinor());
    }

    @Test
    public void iterator() {
        final int COUNT = 10;
        fill_(COUNT);

        assertFalse(fd.isEmpty());
        assertFalse(fd.isGzipped());
        assertEquals(COUNT, fd.size());

        Iterator<TimeSeriesCollection> expected = create_tsdata_().limit(COUNT).iterator();
        Iterator<TimeSeriesCollection> actual = fd.iterator();
        while (expected.hasNext())
            assertEquals(expected.next(), actual.next());
        assertFalse(actual.hasNext());
    }

    @Test
    public void size() {
        assertTrue(fd.isEmpty());

        final int COUNT = 25;
        fill_(COUNT);

        assertFalse(fd.isEmpty());
        assertEquals(COUNT, fd.size());
    }

    @Test
    public void stream() {
        final int COUNT = LARGE_COUNT;
        fill_(COUNT);

        assertFalse(fd.isGzipped());

        Iterator<TimeSeriesCollection> expected = create_tsdata_().limit(COUNT).iterator();
        Iterator<TimeSeriesCollection> actual = fd.stream().iterator();
        while (expected.hasNext())
            assertEquals(expected.next(), actual.next());
        assertFalse(actual.hasNext());
    }

    @Test
    public void streamReversed() {
        final int COUNT = LARGE_COUNT;
        fill_(COUNT);

        assertFalse(fd.isGzipped());

        List<TimeSeriesCollection> expected_list = create_tsdata_().limit(COUNT).collect(Collectors.toList());
        reverse(expected_list);
        Iterator<TimeSeriesCollection> expected = expected_list.iterator();
        Iterator<TimeSeriesCollection> actual = fd.streamReversed().iterator();
        while (expected.hasNext())
            assertEquals(expected.next(), actual.next());
        assertFalse(actual.hasNext());
    }

    @Test
    public void stream_with_begin() {
        final int COUNT = 50;
        fill_(COUNT);

        assertFalse(fd.isEmpty());
        assertFalse(fd.isGzipped());
        assertEquals(COUNT, fd.size());
        final DateTime begin = create_tsdata_().skip(COUNT / 6).findAny().get().getTimestamp();

        Iterator<TimeSeriesCollection> expected = create_tsdata_().limit(COUNT).filter(ts -> !ts.getTimestamp().isBefore(begin)).iterator();
        Iterator<TimeSeriesCollection> actual = fd.stream(begin).iterator();
        while (expected.hasNext())
            assertEquals(expected.next(), actual.next());
        assertFalse(actual.hasNext());
    }

    @Test
    public void stream_with_begin_and_end() {
        final int COUNT = 50;
        fill_(COUNT);

        assertFalse(fd.isEmpty());
        assertFalse(fd.isGzipped());
        assertEquals(COUNT, fd.size());
        final DateTime begin = create_tsdata_().skip(COUNT / 6).findAny().get().getTimestamp();
        final DateTime end = create_tsdata_().skip(5 * COUNT / 6).findAny().get().getTimestamp();

        Iterator<TimeSeriesCollection> expected = create_tsdata_().limit(COUNT)
                .filter(ts -> !ts.getTimestamp().isBefore(begin))
                .filter(ts -> !ts.getTimestamp().isAfter(end))
                .iterator();
        Iterator<TimeSeriesCollection> actual = fd.stream(begin, end).iterator();
        while (expected.hasNext())
            assertEquals(expected.next(), actual.next());
        assertFalse(actual.hasNext());
    }

    @Test
    public void contains() {
        final int COUNT = LARGE_COUNT;
        fill_(COUNT);

        assertTrue(fd.contains(create_tsdata_().skip(COUNT / 2).findFirst().get()));
        assertTrue("contains() visits old files", fd.contains(create_tsdata_().skip(10).findFirst().get()));
        assertTrue("contains() visits new files", fd.contains(create_tsdata_().skip(COUNT - 1).findFirst().get()));
        assertFalse("contains() handles wrong types", fd.contains(new Integer(16)));
    }

    @Test
    public void contains_all() {
        final int COUNT = LARGE_COUNT;
        fill_(COUNT);

        assertTrue(fd.containsAll(create_tsdata_().limit(COUNT).skip(LARGE_COUNT / 4).limit(10).collect(Collectors.toList())));
        assertTrue("containsAll() visits old files", fd.containsAll(create_tsdata_().limit(10).collect(Collectors.toList())));
        assertTrue("containsAll() visits new file", fd.containsAll(create_tsdata_().skip(COUNT - 10).limit(10).collect(Collectors.toList())));
        assertTrue("containsAll() on empty set", fd.containsAll(EMPTY_LIST));
        assertFalse("containsAll() handles wrong types", fd.containsAll(Arrays.asList(new Object(), new Integer(16))));
    }

    @Test
    public void begin_and_end() {
        final int COUNT = 15;
        fill_(COUNT);

        assertEquals(create_tsdata_().findFirst().get().getTimestamp(), fd.getBegin());
        assertEquals(create_tsdata_().skip(COUNT - 1).findFirst().get().getTimestamp(), fd.getEnd());
    }

    @Test
    public void get_keys() throws Exception {
        final int COUNT = LARGE_COUNT;
        fill_(COUNT);

        for (TSDataFileChain.Key key : fd.getKeys()) {
            final TSData file = TSData.readonly(key.getFile());
            assertEquals(file.getBegin(), key.getBegin());
            assertEquals(file.getEnd(), key.getEnd());
        }
    }

    @Test
    public void remove_key() throws Exception {
        final int COUNT = LARGE_COUNT;
        fill_(COUNT);

        final Set<TSDataFileChain.Key> keys = fd.getKeys();
        final TSDataFileChain.Key to_delete = keys.stream().findAny().get();
        final List<TimeSeriesCollection> removed = new ArrayList<>(TSData.readonly(to_delete.getFile()));
        final DateTime removed_begin = removed.stream().map(TimeSeriesCollection::getTimestamp).min(Comparator.naturalOrder()).get();
        final DateTime removed_end = removed.stream().map(TimeSeriesCollection::getTimestamp).max(Comparator.naturalOrder()).get();

        assertTrue(fd.containsAll(removed));
        fd.delete(to_delete);
        assertThat(fd.getKeys(), not(hasItem(to_delete)));
        assertTrue(fd.stream(removed_begin, removed_end).noneMatch(removed::contains));
    }

    @Test(expected = IllegalArgumentException.class)
    public void remove_non_existing_key() throws Exception {
        final int COUNT = LARGE_COUNT;
        fill_(COUNT);

        final TSDataFileChain.Key bad_key = new TSDataFileChain.Key(tmpdir.resolve("I-am-not-here.tsd"), DateTime.now(DateTimeZone.UTC).minusDays(1), DateTime.now(DateTimeZone.UTC));
        fd.delete(bad_key);
    }

    @Test
    public void add() {
        final int COUNT = 7;
        fill_(COUNT);

        final TimeSeriesCollection tsdata = create_tsdata_().skip(COUNT + 1).findFirst().get();

        assertFalse(fd.contains(tsdata));
        fd.add(tsdata);
        assertTrue(fd.contains(tsdata));
    }

    @Test
    public void add_all() {
        final int COUNT = 7;
        fill_(COUNT);

        final List<TimeSeriesCollection> tsdata = create_tsdata_().skip(COUNT + 1).limit(20).collect(Collectors.toList());

        assertTrue(tsdata.stream().noneMatch(fd::contains));
        fd.addAll(tsdata);
        assertTrue(fd.containsAll(tsdata));
    }

    @Test
    public void file_size() throws Exception {
        final int COUNT = LARGE_COUNT;
        fill_(COUNT);

        final long expected = Files.list(tmpdir)
                .mapToLong((path) -> {
                    try {
                        return Files.size(path);
                    } catch (IOException ex) {
                        throw new RuntimeException("test failure", ex);
                    }
                })
                .sum();

        assertEquals(expected, fd.getFileSize());
    }

    @Test
    public void open_existing() throws Exception {
        final int COUNT = 17;
        fill_(COUNT);

        TSDataFileChain opened_fd = TSDataFileChain.openDirExisting(tmpdir).get();

        final Iterator<TimeSeriesCollection> expect = fd.iterator();
        final Iterator<TimeSeriesCollection> actual = fd.iterator();
        while (expect.hasNext())
            assertEquals(expect.next(), actual.next());
        assertFalse(actual.hasNext());

        assertEquals(fd.getKeys(), opened_fd.getKeys());
    }

    @Test
    public void open_empty_dir() throws Exception {
        Optional<TSDataFileChain> opened = TSDataFileChain.openDirExisting(emptydir);

        assertEquals(Optional.empty(), opened);
    }

    /** Generates an endless stream of TimeSeriesCollections. */
    private Stream<TimeSeriesCollection> create_tsdata_() {
        return file_support.create_tsdata(500);
    }

    private void fill_(int count) {
        create_tsdata_().limit(count).forEachOrdered(fd::add);
    }
}
