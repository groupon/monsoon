package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.history.CollectHistory;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ChainingTSCPairTest {
    @Mock
    private CollectHistory history;
    @Mock
    private TimeSeriesCollection current;

    private static final DateTime TS0 = DateTime.now(DateTimeZone.UTC);

    @Before
    public void setup() {
        when(current.getTimestamp()).thenReturn(TS0);
        when(history.streamReversed()).thenReturn(Stream.empty());
    }

    @Test
    public void emptyHistoryTest() {
        ChainingTSCPair tsdata = new ChainingTSCPair(history, ExpressionLookBack.EMPTY) {
            @Override
            public TimeSeriesCollection getCurrentCollection() {
                return current;
            }
        };

        assertEquals(1, tsdata.size());
        assertSame(current, tsdata.getPreviousCollection(0).get());
    }
}
