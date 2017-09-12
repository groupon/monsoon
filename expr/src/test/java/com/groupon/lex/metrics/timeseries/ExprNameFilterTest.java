package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.MetricMatcher;
import com.groupon.lex.metrics.PathMatcher;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class ExprNameFilterTest {
    @Test
    public void noNamesUsed() throws Exception {
        assertEquals(new TimeSeriesMetricFilter(), TimeSeriesMetricExpression.valueOf("true").getNameFilter());
        assertEquals(new TimeSeriesMetricFilter(), TimeSeriesMetricExpression.valueOf("false").getNameFilter());
        assertEquals(new TimeSeriesMetricFilter(), TimeSeriesMetricExpression.valueOf("1.0 / 2").getNameFilter());
        assertEquals(new TimeSeriesMetricFilter(), TimeSeriesMetricExpression.valueOf("\"foo\"").getNameFilter());
    }

    @Test
    public void nameMatch() throws Exception {
        assertEquals(
                new TimeSeriesMetricFilter()
                        .withMetric(new MetricMatcher(
                                new PathMatcher(new PathMatcher.LiteralNameMatch("foo")),
                                new PathMatcher(new PathMatcher.LiteralNameMatch("bar")))),
                TimeSeriesMetricExpression.valueOf("sum(foo bar)").getNameFilter()
        );
    }

    @Test
    public void twoNameMatch() throws Exception {
        assertEquals(
                new TimeSeriesMetricFilter()
                        .withMetric(new MetricMatcher(
                                new PathMatcher(new PathMatcher.LiteralNameMatch("foo")),
                                new PathMatcher(new PathMatcher.LiteralNameMatch("bar"))))
                        .withMetric(new MetricMatcher(
                                new PathMatcher(new PathMatcher.LiteralNameMatch("foobarium")),
                                new PathMatcher(new PathMatcher.LiteralNameMatch("barium")))),
                TimeSeriesMetricExpression.valueOf("foo bar * foobarium barium").getNameFilter()
        );
    }
}
