package com.groupon.lex.metrics.history.v2.xdr;

import static org.junit.Assert.assertArrayEquals;
import org.junit.Test;

public class FromXdrTest {
    @Test
    public void timestampDelta() {
        timestamp_delta td = new timestamp_delta();
        td.first = 100;
        td.delta = new int[]{100, 150};

        long ts[] = FromXdr.timestamp_delta(td);
        assertArrayEquals(new long[]{100, 200, 350}, ts);
    }
}
