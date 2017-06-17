package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.timeseries.ImmutableTimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.ImmutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.SimpleTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.util.Arrays;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class RateExpressionTest {
    @Test
    public void rateIntTest() throws Exception {
        assertEquals(expected(MetricValue.fromDblValue(1)),
                create().apply(createCtx(MetricValue.fromIntValue(60), MetricValue.fromIntValue(59), MetricValue.fromIntValue(0))));
        assertEquals(expected(MetricValue.fromDblValue(1)),
                create("1s").apply(createCtx(MetricValue.fromIntValue(60), MetricValue.fromIntValue(59), MetricValue.fromIntValue(0))));
        assertEquals(expected(MetricValue.fromDblValue(1)),
                create("30s").apply(createCtx(MetricValue.fromIntValue(60), MetricValue.fromIntValue(59), MetricValue.fromIntValue(0))));
        assertEquals(expected(MetricValue.fromDblValue(1)),
                create("1m").apply(createCtx(MetricValue.fromIntValue(60), MetricValue.fromIntValue(59), MetricValue.fromIntValue(0))));
    }

    @Test
    public void rateDblTest() throws Exception {
        assertEquals(expected(MetricValue.fromDblValue(1)),
                create().apply(createCtx(MetricValue.fromDblValue(60), MetricValue.fromDblValue(59), MetricValue.fromDblValue(0))));
        assertEquals(expected(MetricValue.fromDblValue(1)),
                create("1s").apply(createCtx(MetricValue.fromDblValue(60), MetricValue.fromDblValue(59), MetricValue.fromDblValue(0))));
        assertEquals(expected(MetricValue.fromDblValue(1)),
                create("30s").apply(createCtx(MetricValue.fromDblValue(60), MetricValue.fromDblValue(59), MetricValue.fromDblValue(0))));
        assertEquals(expected(MetricValue.fromDblValue(1)),
                create("60s").apply(createCtx(MetricValue.fromDblValue(60), MetricValue.fromDblValue(59), MetricValue.fromDblValue(0))));
    }

    @Test
    public void rateEmptyTest() throws Exception {
        assertEquals(expected(MetricValue.EMPTY),
                create().apply(createCtx(MetricValue.fromDblValue(60), MetricValue.EMPTY, MetricValue.fromDblValue(0))));
        assertEquals(expected(MetricValue.fromDblValue(1)),
                create("60s").apply(createCtx(MetricValue.fromDblValue(60), MetricValue.EMPTY, MetricValue.fromDblValue(0))));
        assertEquals(expected(MetricValue.EMPTY),
                create().apply(createCtx(MetricValue.EMPTY, MetricValue.fromDblValue(59), MetricValue.fromDblValue(0))));
        assertEquals(expected(MetricValue.EMPTY),
                create().apply(createCtx(MetricValue.EMPTY, MetricValue.fromDblValue(59), MetricValue.fromDblValue(0))));
    }

    @Test
    public void rateStringTest() throws Exception {
        assertEquals(expected(MetricValue.EMPTY),
                create().apply(createCtx(MetricValue.fromStrValue("foobar"), MetricValue.fromDblValue(59), MetricValue.fromDblValue(0))));
        assertEquals(expected(MetricValue.EMPTY),
                create().apply(createCtx(MetricValue.fromDblValue(60), MetricValue.fromStrValue("foobar"), MetricValue.fromDblValue(0))));
    }

    @Test
    public void rateHistAndNumberTest() throws Exception {
        assertEquals(expected(MetricValue.EMPTY),
                create().apply(createCtx(MetricValue.fromHistValue(new Histogram(new Histogram.RangeWithCount(0, 1, 1))),
                        MetricValue.fromDblValue(59),
                        MetricValue.fromDblValue(0))));
    }

    @Test
    public void rateHistogramTest() throws Exception {
        assertEquals(expected(MetricValue.fromHistValue(new Histogram(new Histogram.RangeWithCount(0, 1, 1)))),
                create().apply(createCtx(
                        MetricValue.fromHistValue(new Histogram(new Histogram.RangeWithCount(0, 1, 1))),
                        MetricValue.fromHistValue(new Histogram()),
                        MetricValue.EMPTY)));

        assertEquals(expected(MetricValue.fromHistValue(new Histogram(new Histogram.RangeWithCount(0, 1, 1)))),
                create("30s").apply(createCtx(
                MetricValue.fromHistValue(new Histogram(new Histogram.RangeWithCount(0, 1, 60))),
                MetricValue.fromHistValue(new Histogram(new Histogram.RangeWithCount(0, 1, 59))),
                MetricValue.fromHistValue(new Histogram()))));

        assertEquals(expected(MetricValue.fromHistValue(new Histogram(new Histogram.RangeWithCount(0, 1, 1)))),
                create("60s").apply(createCtx(
                MetricValue.fromHistValue(new Histogram(new Histogram.RangeWithCount(0, 1, 61))),
                MetricValue.fromHistValue(new Histogram(new Histogram.RangeWithCount(0, 1, 60))),
                MetricValue.fromHistValue(new Histogram(new Histogram.RangeWithCount(0, 1, 1))))));
    }

    @Test
    public void configStringTest() throws Exception {
        assertEquals("rate(G x)", create().configString().toString());
        assertEquals("rate[1m](G x)", create("60s").configString().toString());
    }

    @Test
    public void lookBackTest() throws Exception {
        assertEquals(Duration.ZERO, create().getLookBack().hintDuration());
        assertEquals(Duration.standardMinutes(5), create("5m").getLookBack().hintDuration());
    }

    private static RateExpression create() throws TimeSeriesMetricExpression.ParseException {
        return (RateExpression) TimeSeriesMetricExpression.valueOf("rate(G x)");
    }

    private static RateExpression create(String duration) throws TimeSeriesMetricExpression.ParseException {
        return (RateExpression) TimeSeriesMetricExpression.valueOf("rate[" + duration + "](G x)");
    }

    private static Context<?> createCtx(MetricValue x0, MetricValue x1, MetricValue x2) {
        final TimeSeriesValue v0 = new ImmutableTimeSeriesValue(GroupName.valueOf("G"), singletonMap(MetricName.valueOf("x"), x0));
        final TimeSeriesValue v1 = new ImmutableTimeSeriesValue(GroupName.valueOf("G"), singletonMap(MetricName.valueOf("x"), x1));
        final TimeSeriesValue v2 = new ImmutableTimeSeriesValue(GroupName.valueOf("G"), singletonMap(MetricName.valueOf("x"), x2));

        final TimeSeriesCollection t0 = new SimpleTimeSeriesCollection(DateTime.now(), singleton(v0));
        final TimeSeriesCollection t1 = new SimpleTimeSeriesCollection(t0.getTimestamp().minus(Duration.standardSeconds(1)), singleton(v1));
        final TimeSeriesCollection t2 = new SimpleTimeSeriesCollection(t0.getTimestamp().minus(Duration.standardSeconds(60)), singleton(v2));
        final TimeSeriesCollectionPair tsdata = new ImmutableTimeSeriesCollectionPair(Arrays.asList(t0, t1, t2));

        return new SimpleContext<>(tsdata, (alert) -> {
        });
    }

    private static TimeSeriesMetricDeltaSet expected(MetricValue v) {
        return new TimeSeriesMetricDeltaSet(singletonMap(Tags.EMPTY, v));
    }
}
