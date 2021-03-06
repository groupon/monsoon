/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.history.xdr.support.FileSupport;
import com.groupon.lex.metrics.history.xdr.support.FileSupport0;
import com.groupon.lex.metrics.history.xdr.support.FileSupport1;
import com.groupon.lex.metrics.timeseries.ImmutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.SimpleTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.reverse;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
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
public class UnmappedReadonlyTSDataFileTest {
    private final FileSupport file_support;
    private List<TimeSeriesCollection> tsdata;
    private List<TimeSeriesCollection> expect_reversed;
    private Path tmpdir, tmpfile;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{new FileSupport(new FileSupport0(), false)},
                new Object[]{new FileSupport(new FileSupport1(), false)},
                new Object[]{new FileSupport(new FileSupport0(), true)},
                new Object[]{new FileSupport(new FileSupport1(), true)}
        );
    }

    public UnmappedReadonlyTSDataFileTest(FileSupport file_support) {
        this.file_support = file_support;
    }

    @Before
    public void setup() throws Exception {
        final DateTime now = new DateTime(DateTimeZone.UTC);

        tsdata = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            tsdata.add(new SimpleTimeSeriesCollection(now.plusMinutes(i), Stream.of(
                    new ImmutableTimeSeriesValue(
                            GroupName.valueOf(SimpleGroupPath.valueOf("foo", "bar")),
                            singletonMap(MetricName.valueOf("x"), MetricValue.fromIntValue(i))))));
        }

        expect_reversed = new ArrayList<>(tsdata);
        reverse(expect_reversed);

        tmpdir = Files.createTempDirectory("monsoon-MmapReadonlyTSDataFileTest");
        tmpdir.toFile().deleteOnExit();
        tmpfile = tmpdir.resolve("test.tsd");
    }

    @After
    public void cleanup() throws Exception {
        Files.deleteIfExists(tmpfile);
    }

    @Test
    public void read() throws Exception {
        file_support.create_file(tmpfile, tsdata);

        final UnmappedReadonlyTSDataFile fd = UnmappedReadonlyTSDataFile.open(tmpfile);

        assertEquals(Files.size(tmpfile), fd.getFileSize());
        assertEquals(tsdata.get(0).getTimestamp(), fd.getBegin());
        assertEquals(tsdata.get(tsdata.size() - 1).getTimestamp(), fd.getEnd());
        assertTrue(fd.getFileChannel().isPresent());
        assertEquals(tsdata.size(), fd.size());
        assertEquals(file_support.getMajor(), fd.getMajor());
        assertEquals(file_support.getMinor(), fd.getMinor());
        assertThat(fd, IsIterableContainingInOrder.contains(tsdata.stream().map(Matchers::equalTo).collect(Collectors.toList())));
        assertThat(fd.streamReversed().collect(Collectors.toList()),
                IsIterableContainingInOrder.contains(expect_reversed.stream().map(Matchers::equalTo).collect(Collectors.toList())));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void cannot_add() throws Exception {
        file_support.create_file(tmpfile, tsdata);

        final UnmappedReadonlyTSDataFile fd = UnmappedReadonlyTSDataFile.open(tmpfile);
        fd.add(tsdata.get(0));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void cannot_add_all() throws Exception {
        file_support.create_file(tmpfile, tsdata);

        final UnmappedReadonlyTSDataFile fd = UnmappedReadonlyTSDataFile.open(tmpfile);
        fd.addAll(tsdata);
    }
}
