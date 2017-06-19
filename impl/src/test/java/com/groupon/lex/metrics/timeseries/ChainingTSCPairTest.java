package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singleton;
import java.util.List;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ChainingTSCPairTest {
    @Mock
    private CollectHistory history;
    @Mock
    private TimeSeriesCollection current;

    private static final DateTime TS0 = DateTime.now(DateTimeZone.UTC);
    private static final GroupName GROUP_NAME = GroupName.valueOf("GROUP");

    private TimeSeriesValue tsv1 = new ImmutableTimeSeriesValue(GROUP_NAME, EMPTY_MAP),
            tsv2 = new ImmutableTimeSeriesValue(GROUP_NAME, EMPTY_MAP);
    private TimeSeriesCollection tsc1 = new SimpleTimeSeriesCollection(TS0.minus(Duration.standardMinutes(1)), singleton(tsv1)),
            tsc2 = new SimpleTimeSeriesCollection(TS0.minus(Duration.standardMinutes(2)), singleton(tsv2));

    @Before
    public void setup() {
        List<TimeSeriesCollection> tsdata = Arrays.asList(tsc1, tsc2);
        List<TimeSeriesCollection> tsdataReversed = new ArrayList<>(tsdata);
        Collections.reverse(tsdataReversed);

        when(current.getTimestamp()).thenReturn(TS0);
        when(history.streamReversed()).thenAnswer((invocation) -> tsdataReversed.stream());
        when(history.stream()).thenAnswer((invocation) -> tsdata.stream());
        when(history.streamGroup(Mockito.any(), Mockito.eq(GROUP_NAME)))
                .thenAnswer((invocation) -> Stream.of(SimpleMapEntry.create(tsc2.getTimestamp(), tsv2), SimpleMapEntry.create(tsc1.getTimestamp(), tsv1)));
    }

    @Test
    public void create() {
        ChainingTSCPair tsdata = new ChainingTSCPair(history, ExpressionLookBack.fromScrapeCount(3)) {
            @Override
            public TimeSeriesCollection getCurrentCollection() {
                return current;
            }
        };

        assertEquals(3, tsdata.size());
        assertSame(current, tsdata.getPreviousCollection(0).get());
        assertEquals(tsc1, tsdata.getPreviousCollection());
        assertEquals(tsc2, tsdata.getPreviousCollection(2).get());
    }
}
