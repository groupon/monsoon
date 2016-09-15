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
package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.TagMatchingClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPairInstance;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import static com.groupon.lex.metrics.timeseries.expression.Checks.testCoercion;
import static com.groupon.lex.metrics.timeseries.expression.Checks.testResult;
import static com.groupon.lex.metrics.timeseries.expression.Util.constantExpression;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class ArithmaticIntegralTest {
    private Context ctx;

    @Before
    public void setup() {
        ctx = new SimpleContext(new TimeSeriesCollectionPairInstance(DateTime.now(DateTimeZone.UTC)), (Alert alert) -> {});
    }

    @Test
    public void constantTest() {
        TimeSeriesMetricDeltaSet result = constantExpression(7).apply(ctx);
        testResult(7, result);
    }

    @Test
    public void zeroIsFalseCoercion() {
        TimeSeriesMetricDeltaSet result = constantExpression(0).apply(ctx);
        testCoercion(false, result);
    }

    @Test
    public void nonZeroIsTrueCoercion() {
        TimeSeriesMetricDeltaSet result = constantExpression(12).apply(ctx);
        testCoercion(true, result);
    }

    @Test
    public void identityExpression() {
        TimeSeriesMetricDeltaSet result = UtilX.identityExpression().apply(constantExpression(7)).apply(ctx);
        testResult(7, result);
    }

    @Test
    public void addition() {
        TimeSeriesMetricDeltaSet result = UtilX.addition().apply(constantExpression(7), constantExpression(5), TagMatchingClause.DEFAULT).apply(ctx);
        testResult(7 + 5, result);
    }

    @Test
    public void subtraction() {
        TimeSeriesMetricDeltaSet result = UtilX.subtraction().apply(constantExpression(7), constantExpression(5), TagMatchingClause.DEFAULT).apply(ctx);
        testResult(7 - 5, result);
    }

    @Test
    public void leftShift() {
        TimeSeriesMetricDeltaSet result = UtilX.leftShift().apply(constantExpression(7), constantExpression(5), TagMatchingClause.DEFAULT).apply(ctx);
        testResult(7 << 5, result);
    }

    @Test
    public void rightShift() {
        TimeSeriesMetricDeltaSet result = UtilX.rightShift().apply(constantExpression(7000), constantExpression(5), TagMatchingClause.DEFAULT).apply(ctx);
        testResult(7000 >> 5, result);
    }

    @Test
    public void multiply() {
        TimeSeriesMetricDeltaSet result = UtilX.multiply().apply(constantExpression(7), constantExpression(5), TagMatchingClause.DEFAULT).apply(ctx);
        testResult(7 * 5, result);
    }

    @Test
    public void divide() {
        TimeSeriesMetricDeltaSet result = UtilX.divide().apply(constantExpression(7), constantExpression(5), TagMatchingClause.DEFAULT).apply(ctx);
        testResult(7 / 5, result);
    }

    @Test
    public void modulo() {
        TimeSeriesMetricDeltaSet result = UtilX.modulo().apply(constantExpression(7), constantExpression(5), TagMatchingClause.DEFAULT).apply(ctx);
        testResult(7 % 5, result);
    }

    @Test
    public void negateNumberExpression() {
        TimeSeriesMetricDeltaSet result = UtilX.negateNumberExpression().apply(constantExpression(7)).apply(ctx);
        testResult(-7, result);
    }

    @Test
    public void numberLessThanPredicate() {
        TimeSeriesMetricDeltaSet result = UtilX.numberLessThanPredicate().apply(constantExpression(7), constantExpression(5), TagMatchingClause.DEFAULT).apply(ctx);
        testResult(7 < 5, result);
    }

    @Test
    public void numberLargerThanPredicate() {
        TimeSeriesMetricDeltaSet result = UtilX.numberLargerThanPredicate().apply(constantExpression(7), constantExpression(5), TagMatchingClause.DEFAULT).apply(ctx);
        testResult(7 > 5, result);
    }

    @Test
    public void numberLessEqualPredicate() {
        TimeSeriesMetricDeltaSet result = UtilX.numberLessEqualPredicate().apply(constantExpression(7), constantExpression(5), TagMatchingClause.DEFAULT).apply(ctx);
        testResult(7 <= 5, result);
    }

    @Test
    public void numberLargerEqualPredicate() {
        TimeSeriesMetricDeltaSet result = UtilX.numberLargerEqualPredicate().apply(constantExpression(7), constantExpression(5), TagMatchingClause.DEFAULT).apply(ctx);
        testResult(7 >= 5, result);
    }

    @Test
    public void numberEqualPredicate() {
        TimeSeriesMetricDeltaSet result = UtilX.equalPredicate().apply(constantExpression(7), constantExpression(5), TagMatchingClause.DEFAULT).apply(ctx);
        testResult(7 == 5, result);
    }

    @Test
    public void numberNotEqualPredicate() {
        TimeSeriesMetricDeltaSet result = UtilX.notEqualPredicate().apply(constantExpression(7), constantExpression(5), TagMatchingClause.DEFAULT).apply(ctx);
        testResult(7 != 5, result);
    }
}
