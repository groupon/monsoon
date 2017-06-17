package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PercentileAggregateExpressionTest {
    @Mock
    private Context<?> ctx;

    @Test
    public void exactTest() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.fromDblValue(50)), create(50, 101).apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.fromDblValue(99)), create(99, 101).apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.fromDblValue(1)), create(50, 3).apply(ctx));
    }

    @Test
    public void inexactTest() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.fromDblValue(49.5)), create(50, 100).apply(ctx));
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.fromDblValue(0.5)), create(50, 2).apply(ctx));
    }

    @Test
    public void ceilTest() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.fromDblValue(17)), create(100, 18).apply(ctx));
    }

    @Test
    public void floorTest() throws Exception {
        assertEquals(new TimeSeriesMetricDeltaSet(MetricValue.fromDblValue(0)), create(0, 18).apply(ctx));
    }

    @Test
    public void configStringTest() throws Exception {
        assertEquals("percentile_agg(50.0, 0, 1, 2)", create(50, 3).configString().toString());
    }

    private static PercentileAggregateExpression create(int p, int count) throws TimeSeriesMetricExpression.ParseException {
        final String expr = IntStream.concat(IntStream.of(p), IntStream.range(0, count))
                .mapToObj(String::valueOf)
                .collect(Collectors.joining(", ", "percentile_agg(", ")"));
        return (PercentileAggregateExpression) TimeSeriesMetricExpression.valueOf(expr);
    }
}
