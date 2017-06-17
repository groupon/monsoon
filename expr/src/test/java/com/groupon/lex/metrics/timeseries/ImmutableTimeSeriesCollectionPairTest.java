package com.groupon.lex.metrics.timeseries;

import java.util.Arrays;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ImmutableTimeSeriesCollectionPairTest {
    @Mock
    private TimeSeriesCollection t0, t1, t2;

    private static final DateTime TS0 = DateTime.now(DateTimeZone.UTC),
            TS1 = TS0.minus(Duration.standardMinutes(1)),
            TS2 = TS1.minus(Duration.standardMinutes(1));

    private TimeSeriesCollectionPair tsdata;

    @Before
    public void setup() {
        tsdata = new ImmutableTimeSeriesCollectionPair(Arrays.asList(t0, t1, t2));

        when(t0.getTimestamp()).thenReturn(TS0);
        when(t1.getTimestamp()).thenReturn(TS1);
        when(t2.getTimestamp()).thenReturn(TS2);
    }

    @Test
    public void getCurrentCollectionTest() {
        assertSame(t0, tsdata.getCurrentCollection());
        verifyZeroInteractions(t0, t1, t2);
    }

    @Test
    public void getPreviousCollectionTest() {
        assertSame(t1, tsdata.getPreviousCollection());
        assertSame(t0, tsdata.getPreviousCollection(0).get());
        assertSame(t1, tsdata.getPreviousCollection(1).get());
        assertSame(t2, tsdata.getPreviousCollection(2).get());
        verifyZeroInteractions(t0, t1, t2);
    }

    @Test
    public void sizeTest() {
        assertEquals(3, tsdata.size());
        verifyZeroInteractions(t0, t1, t2);
    }

    @Test
    public void getPreviousCollectionAtTest() {
        assertEquals(TS0, tsdata.getPreviousCollectionAt(TS0).getTimestamp());
        assertEquals(TS1, tsdata.getPreviousCollectionAt(TS1).getTimestamp());
        assertEquals(TS2, tsdata.getPreviousCollectionAt(TS2).getTimestamp());
        assertEquals(TS0.minus(Duration.standardSeconds(30)), tsdata.getPreviousCollectionAt(TS0.minus(Duration.standardSeconds(30))).getTimestamp());
    }

    @Test
    public void getCollectionIntervalTest() {
        assertEquals(Duration.standardMinutes(1), tsdata.getCollectionInterval());
    }
}
