package com.groupon.lex.metrics.timeseries;

import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class AbstractTSCPairTest {
    private static final Logger LOG = Logger.getLogger(AbstractTSCPairTest.class.getName());

    private static class Impl extends AbstractTSCPair {
        public TimeSeriesCollection current = new MutableTimeSeriesCollection();

        @Override
        public TimeSeriesCollection getCurrentCollection() {
            return current;
        }

        public String debugString() {
            return "Impl{current=" + current + ", " + toString() + "}";
        }
    }

    private DateTime now;
    private Impl impl;
    private List<MutableTimeSeriesCollection> input;

    private void setup(ExpressionLookBack lookback) {
        impl = new Impl();

        now = new DateTime(DateTimeZone.UTC);
        LOG.log(Level.INFO, "now = {0}", now);
        input = unmodifiableList(Stream.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .map(Duration::standardMinutes)
                .map(now::minus)
                .map(MutableTimeSeriesCollection::new)
                .collect(Collectors.toList()));
        LOG.log(Level.INFO, "input = {0}", input);
        impl.current = input.get(0);
        Stream.of(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
                .map(input::get)
                .peek(tsc -> LOG.log(Level.INFO, "impl.update({0})", tsc))
                .forEach(tsc -> impl.update(tsc, lookback));
        LOG.log(Level.INFO, "impl = {0}", impl.debugString());
    }

    @Test
    public void empty_lookback() {
        setup(ExpressionLookBack.EMPTY);

        assertSame(input.get(0), impl.getCurrentCollection());
        assertEquals(input.get(1).getTimestamp(), impl.getPreviousCollection().getTimestamp());
        assertSame(input.get(0), impl.getPreviousCollection(0).get());
        assertEquals(input.get(1).getTimestamp(), impl.getPreviousCollection(1).get().getTimestamp());
        assertEquals(Optional.empty(), impl.getPreviousCollection(2));
        assertEquals(Optional.empty(), impl.getPreviousCollection(3));
    }

    @Test
    public void scrapeCount_lookback() {
        setup(ExpressionLookBack.fromScrapeCount(2));

        assertSame(input.get(0), impl.getCurrentCollection());
        assertEquals(input.get(1).getTimestamp(), impl.getPreviousCollection().getTimestamp());
        assertSame(input.get(0), impl.getPreviousCollection(0).get());
        assertEquals(input.get(1).getTimestamp(), impl.getPreviousCollection(1).get().getTimestamp());
        assertEquals(input.get(2).getTimestamp(), impl.getPreviousCollection(2).get().getTimestamp());
        assertEquals(Optional.empty(), impl.getPreviousCollection(3));
        assertEquals(Optional.empty(), impl.getPreviousCollection(4));
        assertEquals(Optional.empty(), impl.getPreviousCollection(5));
        assertEquals(Optional.empty(), impl.getPreviousCollection(6));
    }

    @Test
    public void previousPair() {
        setup(ExpressionLookBack.fromScrapeCount(5));

        // Test 0 lookback, as it is a special case
        assertEquals(input.get(0).getTimestamp(), impl.getPreviousCollectionPair(0).getCurrentCollection().getTimestamp());
        assertEquals(input.get(1).getTimestamp(), impl.getPreviousCollectionPair(0).getPreviousCollection().getTimestamp());
        assertEquals(input.get(1).getTimestamp(), impl.getPreviousCollectionPair(0).getPreviousCollection(1).get().getTimestamp());
        assertEquals(input.get(2).getTimestamp(), impl.getPreviousCollectionPair(0).getPreviousCollection(2).get().getTimestamp());
        assertEquals(input.get(5).getTimestamp(), impl.getPreviousCollectionPair(0).getPreviousCollection(5).get().getTimestamp());
        assertEquals(Optional.empty(), impl.getPreviousCollectionPair(0).getPreviousCollection(6));

        // Test 2 lookback
        assertEquals(input.get(2).getTimestamp(), impl.getPreviousCollectionPair(2).getCurrentCollection().getTimestamp());
        assertEquals(input.get(3).getTimestamp(), impl.getPreviousCollectionPair(2).getPreviousCollection().getTimestamp());
        assertEquals(input.get(3).getTimestamp(), impl.getPreviousCollectionPair(2).getPreviousCollection(1).get().getTimestamp());
        assertEquals(input.get(4).getTimestamp(), impl.getPreviousCollectionPair(2).getPreviousCollection(2).get().getTimestamp());
        assertEquals(input.get(5).getTimestamp(), impl.getPreviousCollectionPair(2).getPreviousCollection(3).get().getTimestamp());
        assertEquals(Optional.empty(), impl.getPreviousCollectionPair(2).getPreviousCollection(4));
    }

    @Test
    public void previousPair_using_exactInterval() {
        final Duration delta0 = Duration.ZERO;
        final Duration delta2 = Duration.standardMinutes(2);
        setup(ExpressionLookBack.fromScrapeCount(5));

        // Test 0 lookback, as it is a special case
        assertEquals(input.get(0).getTimestamp(), impl.getPreviousCollectionPair(delta0).getCurrentCollection().getTimestamp());
        assertEquals(input.get(1).getTimestamp(), impl.getPreviousCollectionPair(delta0).getPreviousCollection().getTimestamp());
        assertEquals(input.get(1).getTimestamp(), impl.getPreviousCollectionPair(delta0).getPreviousCollection(1).get().getTimestamp());
        assertEquals(input.get(2).getTimestamp(), impl.getPreviousCollectionPair(delta0).getPreviousCollection(2).get().getTimestamp());
        assertEquals(input.get(5).getTimestamp(), impl.getPreviousCollectionPair(delta0).getPreviousCollection(5).get().getTimestamp());
        assertEquals(Optional.empty(), impl.getPreviousCollectionPair(delta0).getPreviousCollection(6));

        // Test 2 lookback
        assertEquals(input.get(2).getTimestamp(), impl.getPreviousCollectionPair(delta2).getCurrentCollection().getTimestamp());
        assertEquals(input.get(3).getTimestamp(), impl.getPreviousCollectionPair(delta2).getPreviousCollection().getTimestamp());
        assertEquals(input.get(3).getTimestamp(), impl.getPreviousCollectionPair(delta2).getPreviousCollection(1).get().getTimestamp());
        assertEquals(input.get(4).getTimestamp(), impl.getPreviousCollectionPair(delta2).getPreviousCollection(2).get().getTimestamp());
        assertEquals(input.get(5).getTimestamp(), impl.getPreviousCollectionPair(delta2).getPreviousCollection(3).get().getTimestamp());
        assertEquals(Optional.empty(), impl.getPreviousCollectionPair(delta2).getPreviousCollection(4));
    }

    @Test
    public void previousPair_using_inexactInterval() {
        final Duration delta2 = Duration.standardMinutes(2).minus(Duration.standardSeconds(12));
        setup(ExpressionLookBack.fromScrapeCount(5));

        // Test 2 lookback
        assertEquals(input.get(2).getTimestamp(), impl.getPreviousCollectionPair(delta2).getCurrentCollection().getTimestamp());
        assertEquals(input.get(3).getTimestamp(), impl.getPreviousCollectionPair(delta2).getPreviousCollection().getTimestamp());
        assertEquals(input.get(3).getTimestamp(), impl.getPreviousCollectionPair(delta2).getPreviousCollection(1).get().getTimestamp());
        assertEquals(input.get(4).getTimestamp(), impl.getPreviousCollectionPair(delta2).getPreviousCollection(2).get().getTimestamp());
        assertEquals(input.get(5).getTimestamp(), impl.getPreviousCollectionPair(delta2).getPreviousCollection(3).get().getTimestamp());
        assertEquals(Optional.empty(), impl.getPreviousCollectionPair(delta2).getPreviousCollection(4));
    }

    @Test
    public void previousPairsSince_exact() {
        final Duration delta2 = Duration.standardMinutes(2);
        setup(ExpressionLookBack.fromScrapeCount(5));

        assertEquals("this test checks that the oldest collection has the same timestamp as was requested and is included",
                input.get(2).getTimestamp(), input.get(0).getTimestamp().minus(delta2));

        List<TimeSeriesCollectionPair> pairs = impl.getCollectionPairsSince(delta2);

        // 2 minutes in the past
        assertEquals(input.get(2).getTimestamp(), pairs.get(0).getPreviousCollection(0).get().getTimestamp());
        assertEquals(input.get(3).getTimestamp(), pairs.get(0).getPreviousCollection(1).get().getTimestamp());
        assertEquals(input.get(4).getTimestamp(), pairs.get(0).getPreviousCollection(2).get().getTimestamp());
        assertEquals(input.get(5).getTimestamp(), pairs.get(0).getPreviousCollection(3).get().getTimestamp());
        assertEquals(Optional.empty(), pairs.get(0).getPreviousCollection(4));

        // 1 minute in the past
        assertEquals(input.get(1).getTimestamp(), pairs.get(1).getPreviousCollection(0).get().getTimestamp());
        assertEquals(input.get(2).getTimestamp(), pairs.get(1).getPreviousCollection(1).get().getTimestamp());
        assertEquals(input.get(3).getTimestamp(), pairs.get(1).getPreviousCollection(2).get().getTimestamp());
        assertEquals(input.get(4).getTimestamp(), pairs.get(1).getPreviousCollection(3).get().getTimestamp());
        assertEquals(input.get(5).getTimestamp(), pairs.get(1).getPreviousCollection(4).get().getTimestamp());
        assertEquals(Optional.empty(), pairs.get(1).getPreviousCollection(5));

        // current collection
        assertEquals(input.get(0).getTimestamp(), pairs.get(2).getPreviousCollection(0).get().getTimestamp());
        assertEquals(input.get(1).getTimestamp(), pairs.get(2).getPreviousCollection(1).get().getTimestamp());
        assertEquals(input.get(2).getTimestamp(), pairs.get(2).getPreviousCollection(2).get().getTimestamp());
        assertEquals(input.get(3).getTimestamp(), pairs.get(2).getPreviousCollection(3).get().getTimestamp());
        assertEquals(input.get(4).getTimestamp(), pairs.get(2).getPreviousCollection(4).get().getTimestamp());
        assertEquals(input.get(5).getTimestamp(), pairs.get(2).getPreviousCollection(5).get().getTimestamp());
        assertEquals(Optional.empty(), pairs.get(2).getPreviousCollection(6));

        assertEquals(3, pairs.size());
    }

    @Test
    public void previousPairsSince_inexact() {
        final Duration delta2 = Duration.standardMinutes(2).plus(Duration.standardSeconds(17));
        setup(ExpressionLookBack.fromScrapeCount(5));

        assertTrue("this test checks that the oldest collection has the first timestamp after what was requested",
                input.get(2).getTimestamp().isAfter(input.get(0).getTimestamp().minus(delta2)));
        assertTrue("this test checks that the oldest collection has the first timestamp after what was requested",
                input.get(3).getTimestamp().isBefore(input.get(0).getTimestamp().minus(delta2)));

        List<TimeSeriesCollectionPair> pairs = impl.getCollectionPairsSince(delta2);

        // 2 minutes in the past
        assertEquals(input.get(2).getTimestamp(), pairs.get(0).getPreviousCollection(0).get().getTimestamp());
        assertEquals(input.get(3).getTimestamp(), pairs.get(0).getPreviousCollection(1).get().getTimestamp());
        assertEquals(input.get(4).getTimestamp(), pairs.get(0).getPreviousCollection(2).get().getTimestamp());
        assertEquals(input.get(5).getTimestamp(), pairs.get(0).getPreviousCollection(3).get().getTimestamp());
        assertEquals(Optional.empty(), pairs.get(0).getPreviousCollection(4));

        // 1 minute in the past
        assertEquals(input.get(1).getTimestamp(), pairs.get(1).getPreviousCollection(0).get().getTimestamp());
        assertEquals(input.get(2).getTimestamp(), pairs.get(1).getPreviousCollection(1).get().getTimestamp());
        assertEquals(input.get(3).getTimestamp(), pairs.get(1).getPreviousCollection(2).get().getTimestamp());
        assertEquals(input.get(4).getTimestamp(), pairs.get(1).getPreviousCollection(3).get().getTimestamp());
        assertEquals(input.get(5).getTimestamp(), pairs.get(1).getPreviousCollection(4).get().getTimestamp());
        assertEquals(Optional.empty(), pairs.get(1).getPreviousCollection(5));

        // current collection
        assertEquals(input.get(0).getTimestamp(), pairs.get(2).getPreviousCollection(0).get().getTimestamp());
        assertEquals(input.get(1).getTimestamp(), pairs.get(2).getPreviousCollection(1).get().getTimestamp());
        assertEquals(input.get(2).getTimestamp(), pairs.get(2).getPreviousCollection(2).get().getTimestamp());
        assertEquals(input.get(3).getTimestamp(), pairs.get(2).getPreviousCollection(3).get().getTimestamp());
        assertEquals(input.get(4).getTimestamp(), pairs.get(2).getPreviousCollection(4).get().getTimestamp());
        assertEquals(input.get(5).getTimestamp(), pairs.get(2).getPreviousCollection(5).get().getTimestamp());
        assertEquals(Optional.empty(), pairs.get(2).getPreviousCollection(6));

        assertEquals(3, pairs.size());
    }

    @Test
    public void previous_using_tooLargeInterval() {
        setup(ExpressionLookBack.fromScrapeCount(10));

        assertNotNull(impl.getPreviousCollectionPair(Duration.standardDays(1)));
        assertEquals(Optional.empty(), impl.getPreviousCollection(Duration.standardDays(1)));
    }

    @Test
    public void previousAt_exact() {
        final Duration delta2 = Duration.standardMinutes(2);
        setup(ExpressionLookBack.fromScrapeCount(10));

        assertEquals(input.get(2).getTimestamp(), impl.getPreviousCollectionAt(delta2).getTimestamp());
    }

    @Test
    public void previousAt_inexact() {
        final Duration delta2 = Duration.standardMinutes(2).plus(Duration.standardSeconds(30));
        setup(ExpressionLookBack.fromScrapeCount(10));

        assertEquals(input.get(0).getTimestamp().minus(delta2), impl.getPreviousCollectionAt(delta2).getTimestamp());
    }
}
