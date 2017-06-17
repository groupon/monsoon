package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InequalityPredicateTest {
    @Mock
    private Context<?> ctx;

    @Test
    public void testBoolBool() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("true != true").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("false != false").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("false != true").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("true != false").apply(ctx));
    }

    @Test
    public void testBoolInt() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("true != 1").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("false != 0").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("true != 17").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("false != 17").apply(ctx));
    }

    @Test
    public void testBoolFloat() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("true != 1.0").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("false != 0.0").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("true != 17e1").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("false != 17e0").apply(ctx));
    }

    @Test
    public void testBoolString() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("true != \"true\"").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("false != \"false\"").apply(ctx));
    }

    @Test
    public void testBoolHistogram() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("true != [0..1=1]").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("false != []").apply(ctx));
    }

    @Test
    public void testIntBool() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("1 != true").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("0 != false").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("17 != true").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("17 != false").apply(ctx));
    }

    @Test
    public void testIntInt() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("1 != 1").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("0 != 0").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("17 != 16").apply(ctx));
    }

    @Test
    public void testIntFloat() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("1 != 1.0").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("0 != 0.0").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("17 != 16.0").apply(ctx));
    }

    @Test
    public void testIntString() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("17 != \"true\"").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("0 != \"false\"").apply(ctx));
    }

    @Test
    public void testIntHistogram() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("17 != [0..1=1]").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("0 != []").apply(ctx));
    }

    @Test
    public void testFloatBool() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("1.0 != true").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("0.0 != false").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("17.1 != true").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("17.2 != false").apply(ctx));
    }

    @Test
    public void testFloatInt() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("1.0 != 1").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("0.0 != 0").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("16.1 != 16").apply(ctx));
    }

    @Test
    public void testFloatFloat() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("1.0 != 1.0").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("0.0 != 0.0").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("16.1 != 16.0").apply(ctx));
    }

    @Test
    public void testFloatString() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("17.0 != \"17.0\"").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("0.0 != \"0.0\"").apply(ctx));
    }

    @Test
    public void testFloatHistogram() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("17.0 != [0..1=1]").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("0.0 != []").apply(ctx));
    }

    @Test
    public void testStringBool() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("\"foo\" != true").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("\"foo\" != false").apply(ctx));
    }

    @Test
    public void testStringInt() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("\"\" != 0").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("\"foo\" != 3").apply(ctx));
    }

    @Test
    public void testStringFloat() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("\"\" != 0.0").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("\"foo\" != 3.14").apply(ctx));
    }

    @Test
    public void testStringString() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("\"foo\" != \"foo\"").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("\"foo\" != \"bar\"").apply(ctx));
    }

    @Test
    public void testStringHistogram() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("\"foo\" != [0..3=1]").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("\"foo\" != []").apply(ctx));
    }

    @Test
    public void testHistogramBool() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("[0..3=1] != true").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("[] != false").apply(ctx));
    }

    @Test
    public void testHistogramInt() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("[0..3=1] != 3").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("[] != 0").apply(ctx));
    }

    @Test
    public void testHistogramFloat() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("[0..3=1] != 3.13").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("[] != 0.1").apply(ctx));
    }

    @Test
    public void testHistogramString() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("[0..3=1] != \"0123\"").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.EMPTY), create("[] != \"\"").apply(ctx));
    }

    @Test
    public void testHistogramHistogram() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.FALSE), create("[0..3=3] != [0..1=1, 1..2=1, 2..3=1]").apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.TRUE), create("[] != [0..1=1]").apply(ctx));
    }

    private static InequalityPredicate create(String expr) throws TimeSeriesMetricExpression.ParseException {
        return (InequalityPredicate) TimeSeriesMetricExpression.valueOf(expr);
    }
}
