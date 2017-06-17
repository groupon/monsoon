package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Checks.testResult;
import static com.groupon.lex.metrics.timeseries.expression.Checks.testResultEmpty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ArithmaticHistogramTest {
    @Mock
    private Context<?> ctx;

    @Test
    public void constantTest() throws Exception {
        TimeSeriesMetricDeltaSet result = TimeSeriesMetricExpression.valueOf("[2..5=7]").apply(ctx);
        testResult(new Histogram(new Histogram.RangeWithCount(2, 5, 7)), result);
    }

    @Test
    public void addition() throws Exception {
        TimeSeriesMetricDeltaSet result = TimeSeriesMetricExpression.valueOf("[0..1=2] + [1..5=8]").apply(ctx);
        testResult(new Histogram(new Histogram.RangeWithCount(0, 5, 10)), result);

        testResultEmpty(TimeSeriesMetricExpression.valueOf("[0..1=2] + 1").apply(ctx));
        testResultEmpty(TimeSeriesMetricExpression.valueOf("2 + [0..1=2]").apply(ctx));
    }

    @Test
    public void subtraction() throws Exception {
        TimeSeriesMetricDeltaSet result = TimeSeriesMetricExpression.valueOf("[0..1=2] - [0..1=1]").apply(ctx);
        testResult(new Histogram(new Histogram.RangeWithCount(0, 1, 1)), result);

        testResultEmpty(TimeSeriesMetricExpression.valueOf("[0..1=2] - 1").apply(ctx));
        testResultEmpty(TimeSeriesMetricExpression.valueOf("1 - [0..1=2]").apply(ctx));
    }

    @Test
    public void multiplication() throws Exception {
        TimeSeriesMetricDeltaSet resultSuffixed = TimeSeriesMetricExpression.valueOf("[0..1=2] * 4.0").apply(ctx);
        testResult(new Histogram(new Histogram.RangeWithCount(0, 1, 8)), resultSuffixed);

        TimeSeriesMetricDeltaSet resultPrefixed = TimeSeriesMetricExpression.valueOf("3 * [0..1=2]").apply(ctx);
        testResult(new Histogram(new Histogram.RangeWithCount(0, 1, 6)), resultPrefixed);
    }

    @Test
    public void division() throws Exception {
        TimeSeriesMetricDeltaSet result = TimeSeriesMetricExpression.valueOf("[0..1=2] / 4.0").apply(ctx);
        testResult(new Histogram(new Histogram.RangeWithCount(0, 1, 0.5)), result);

        testResultEmpty(TimeSeriesMetricExpression.valueOf("3 / [0..1=2]").apply(ctx));
    }
}
