package com.groupon.lex.metrics.timeseries;

import java.util.Arrays;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TimeSeriesCollectionPairTest {
    @Mock
    private TimeSeriesCollection t0, t1, t2;
    @Mock
    private TimeSeriesCollectionPair parent;

    private static final DateTime TS0 = DateTime.now(DateTimeZone.UTC),
            TS1 = TS0.minus(Duration.standardMinutes(1)),
            TS2 = TS1.minus(Duration.standardMinutes(1));

    private List<TimeSeriesCollection> list;

    @Before
    public void setup() {
        list = unmodifiableList(Arrays.asList(t0, t1, t2));

        when(t0.getTimestamp()).thenReturn(TS0);
        when(t1.getTimestamp()).thenReturn(TS1);
        when(t2.getTimestamp()).thenReturn(TS2);
    }

    @Test
    public void getPreviousCollectionAtTest() {
        assertEquals(TS0, TimeSeriesCollectionPair.getPreviousCollectionAt(list, TS0).getTimestamp());
        assertEquals(TS1, TimeSeriesCollectionPair.getPreviousCollectionAt(list, TS1).getTimestamp());
        assertEquals(TS2, TimeSeriesCollectionPair.getPreviousCollectionAt(list, TS2).getTimestamp());
        assertEquals(TS0.minus(Duration.standardSeconds(30)), TimeSeriesCollectionPair.getPreviousCollectionAt(list, TS0.minus(Duration.standardSeconds(30))).getTimestamp());
        assertEquals(TS0.minus(Duration.standardDays(10)), TimeSeriesCollectionPair.getPreviousCollectionAt(list, TS0.minus(Duration.standardDays(10))).getTimestamp());
        assertEquals(TS0.plus(Duration.standardDays(10)), TimeSeriesCollectionPair.getPreviousCollectionAt(list, TS0.plus(Duration.standardDays(10))).getTimestamp());
    }

    @Test
    public void getPreviousCollectionPairAtTest() {
        assertEquals(TS0, TimeSeriesCollectionPair.getPreviousCollectionPairAt(list, TS0, parent).getCurrentCollection().getTimestamp());
        assertEquals(TS1, TimeSeriesCollectionPair.getPreviousCollectionPairAt(list, TS1, parent).getCurrentCollection().getTimestamp());
        assertEquals(TS2, TimeSeriesCollectionPair.getPreviousCollectionPairAt(list, TS2, parent).getCurrentCollection().getTimestamp());
        assertEquals(TS0.minus(Duration.standardSeconds(30)), TimeSeriesCollectionPair.getPreviousCollectionPairAt(list, TS0.minus(Duration.standardSeconds(30)), parent).getCurrentCollection().getTimestamp());
        assertEquals(TS0.minus(Duration.standardDays(10)), TimeSeriesCollectionPair.getPreviousCollectionPairAt(list, TS0.minus(Duration.standardDays(10)), parent).getCurrentCollection().getTimestamp());

        assertSame(t1, TimeSeriesCollectionPair.getPreviousCollectionPairAt(list, TS0, parent).getPreviousCollection());
        assertSame(t2, TimeSeriesCollectionPair.getPreviousCollectionPairAt(list, TS0, parent).getPreviousCollection(2).get());
    }
}
