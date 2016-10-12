/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history;

import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class HistoryContextTest {
    private static final Logger LOG = Logger.getLogger(HistoryContextTest.class.getName());
    private DateTime now;
    private List<TimeSeriesCollection> input;
    private TimeSeriesCollection dummy;

    @Before
    public void setup() {
        now = new DateTime(DateTimeZone.UTC);
        input = unmodifiableList(Stream.of(10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
                .map(Duration::standardMinutes)
                .map(now::minus)
                .map(MutableTimeSeriesCollection::new)
                .collect(Collectors.toList()));
        dummy = new MutableTimeSeriesCollection(now.minus(Duration.standardDays(7)));
        LOG.log(Level.INFO, "now={0}", now);
        LOG.log(Level.INFO, "input={0}", input);
    }

    @Test
    public void visit_in_order() {
        final List<TimeSeriesCollection> visited = HistoryContext
                .stream(input, ExpressionLookBack.EMPTY)
                .map(ctx -> ctx.getTSData().getCurrentCollection())
                .collect(Collectors.toList());
        LOG.log(Level.INFO, "visited={0}", visited);

        assertSame(input.get( 0), visited.get( 0));
        assertSame(input.get( 1), visited.get( 1));
        assertSame(input.get( 2), visited.get( 2));
        assertSame(input.get( 3), visited.get( 3));
        assertSame(input.get( 4), visited.get( 4));
        assertSame(input.get( 5), visited.get( 5));
        assertSame(input.get( 6), visited.get( 6));
        assertSame(input.get( 7), visited.get( 7));
        assertSame(input.get( 8), visited.get( 8));
        assertSame(input.get( 9), visited.get( 9));
        assertSame(input.get(10), visited.get(10));
    }

    @Test
    public void previous_is_remembered() {
        final List<TimeSeriesCollection> visited = HistoryContext
                .stream(input, ExpressionLookBack.EMPTY)
                .map(ctx -> ctx.getTSData().getPreviousCollection())
                .collect(Collectors.toList());
        LOG.log(Level.INFO, "visited={0}", visited);

        assertEquals(input.get( 0).getTimestamp(), visited.get( 1).getTimestamp());
        assertEquals(input.get( 1).getTimestamp(), visited.get( 2).getTimestamp());
        assertEquals(input.get( 2).getTimestamp(), visited.get( 3).getTimestamp());
        assertEquals(input.get( 3).getTimestamp(), visited.get( 4).getTimestamp());
        assertEquals(input.get( 4).getTimestamp(), visited.get( 5).getTimestamp());
        assertEquals(input.get( 5).getTimestamp(), visited.get( 6).getTimestamp());
        assertEquals(input.get( 6).getTimestamp(), visited.get( 7).getTimestamp());
        assertEquals(input.get( 7).getTimestamp(), visited.get( 8).getTimestamp());
        assertEquals(input.get( 8).getTimestamp(), visited.get( 9).getTimestamp());
        assertEquals(input.get( 9).getTimestamp(), visited.get(10).getTimestamp());
    }

    @Test
    public void can_look_back_1() {
        final List<DateTime> visited = HistoryContext
                .stream(input, ExpressionLookBack.fromScrapeCount(2))
                .map(ctx -> ctx.getTSData().getPreviousCollection(1).orElse(dummy).getTimestamp())
                .collect(Collectors.toList());
        LOG.log(Level.INFO, "visited.size()={0}", visited.size());
        LOG.log(Level.INFO, "visited={0}", visited);

        assertEquals(input.get( 0).getTimestamp(), visited.get( 1));
        assertEquals(input.get( 1).getTimestamp(), visited.get( 2));
        assertEquals(input.get( 2).getTimestamp(), visited.get( 3));
        assertEquals(input.get( 3).getTimestamp(), visited.get( 4));
        assertEquals(input.get( 4).getTimestamp(), visited.get( 5));
        assertEquals(input.get( 5).getTimestamp(), visited.get( 6));
        assertEquals(input.get( 6).getTimestamp(), visited.get( 7));
        assertEquals(input.get( 7).getTimestamp(), visited.get( 8));
        assertEquals(input.get( 8).getTimestamp(), visited.get( 9));
        assertEquals(input.get( 9).getTimestamp(), visited.get(10));
    }

    @Test
    public void can_look_back_2() {
        final List<TimeSeriesCollection> visited = HistoryContext
                .stream(input, ExpressionLookBack.fromScrapeCount(2))
                .map(ctx -> ctx.getTSData().getPreviousCollection(2).orElse(dummy))
                .collect(Collectors.toList());
        LOG.log(Level.INFO, "visited={0}", visited);

        assertEquals(input.get( 0).getTimestamp(), visited.get( 2).getTimestamp());
        assertEquals(input.get( 1).getTimestamp(), visited.get( 3).getTimestamp());
        assertEquals(input.get( 2).getTimestamp(), visited.get( 4).getTimestamp());
        assertEquals(input.get( 3).getTimestamp(), visited.get( 5).getTimestamp());
        assertEquals(input.get( 4).getTimestamp(), visited.get( 6).getTimestamp());
        assertEquals(input.get( 5).getTimestamp(), visited.get( 7).getTimestamp());
        assertEquals(input.get( 6).getTimestamp(), visited.get( 8).getTimestamp());
        assertEquals(input.get( 7).getTimestamp(), visited.get( 9).getTimestamp());
        assertEquals(input.get( 8).getTimestamp(), visited.get(10).getTimestamp());
    }
}
