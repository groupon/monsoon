package com.groupon.lex.metrics.history.v2.xdr;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class ToXdrTest {
    @Test
    public void timestampDelta() {
        long ts[] = new long[]{100, 200, 350};
        timestamp_delta td = ToXdr.timestamp_delta(ts);

        assertEquals(100l, td.first);
        assertArrayEquals(new int[]{100, 150}, td.delta);
    }
}
