/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.groupon.lex.metrics.history;

import com.groupon.lex.metrics.timeseries.EmptyTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.InterpolatedTSC;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.util.ArrayList;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.reverse;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.junit.Before;
import org.junit.Test;

public class WindowedTSCIteratorTest {
    private List<TimeSeriesCollection> collections;
    private List<TimeSeriesCollection> expected;
    private static final Duration lookBack = Duration.standardHours(3);
    private static final Duration lookForward = Duration.standardHours(2);

    @Before
    public void setup() {
        final DateTime ts = new DateTime(1970, 1, 1, 0, 0, DateTimeZone.UTC);

        collections = new ArrayList<>();
        expected = new ArrayList<>();
        for (int i = 0; i < 4; ++i)
            collections.add(new EmptyTimeSeriesCollection(ts.plus(Duration.standardHours(i))));

        for (int i = 0; i < collections.size(); ++i) {
            TimeSeriesCollection c = collections.get(i);
            List<TimeSeriesCollection> past = collections.stream()
                    .filter(elem -> !elem.getTimestamp().isBefore(c.getTimestamp().minus(lookBack)))
                    .filter(elem -> elem.getTimestamp().isBefore(c.getTimestamp()))
                    .collect(Collectors.toList());
            reverse(past);
            List<TimeSeriesCollection> future = collections.stream()
                    .filter(elem -> !elem.getTimestamp().isAfter(c.getTimestamp().plus(lookForward)))
                    .filter(elem -> elem.getTimestamp().isAfter(c.getTimestamp()))
                    .collect(Collectors.toList());
            expected.add(new InterpolatedTSC(c, past, future));
        }
    }

    @Test
    public void constructor() {
        final List<TimeSeriesCollection> result = new ArrayList<>();
        new WindowedTSCIterator(collections.iterator(), lookBack, lookForward)
                .forEachRemaining(result::add);

        assertEquals(expected, result);
    }

    @Test
    public void emptyInputCollectionHasNext() {
        WindowedTSCIterator iter = new WindowedTSCIterator(emptyIterator(), lookBack, lookForward);

        assertFalse(iter.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void emptyInputCollectionNext() {
        WindowedTSCIterator iter = new WindowedTSCIterator(emptyIterator(), lookBack, lookForward);

        iter.next();
    }

    @Test(expected = NullPointerException.class)
    public void constructorNullIterator() {
        new WindowedTSCIterator(null, lookBack, lookForward);
    }

    @Test(expected = NullPointerException.class)
    public void constructorNullLookBack() {
        new WindowedTSCIterator(emptyIterator(), null, lookForward);
    }

    @Test(expected = NullPointerException.class)
    public void constructorNullLookForward() {
        new WindowedTSCIterator(emptyIterator(), lookBack, null);
    }

    @Test
    public void stream() {
        final List<TimeSeriesCollection> result = WindowedTSCIterator.stream(collections.stream(), lookBack, lookForward)
                .collect(Collectors.toList());

        assertEquals(expected, result);
    }
}
