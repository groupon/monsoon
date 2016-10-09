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
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Checks.testCoercion;
import static com.groupon.lex.metrics.timeseries.expression.Checks.testResult;
import static com.groupon.lex.metrics.timeseries.expression.Checks.testResultEmpty;
import static com.groupon.lex.metrics.timeseries.expression.Util.constantExpression;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class BooleanExprTest {
    private Context<?> ctx;

    @Before
    public void setup() {
        ctx = new SimpleContext<>(new TimeSeriesCollectionPairInstance(DateTime.now(DateTimeZone.UTC)), (Alert alert) -> {});
    }

    @Test
    public void trueIsTrue() {
        testResult(true, TimeSeriesMetricExpression.TRUE.apply(ctx));
    }

    @Test
    public void falseIsFalse() {
        testResult(false, TimeSeriesMetricExpression.FALSE.apply(ctx));
    }

    @Test
    public void identityExpression() {
        testResult(false, UtilX.identityExpression().apply(TimeSeriesMetricExpression.FALSE).apply(ctx));
        testResult(true, UtilX.identityExpression().apply(TimeSeriesMetricExpression.TRUE).apply(ctx));
    }

    @Test
    public void negateBooleanPredicate() {
        testResult(false, UtilX.negateBooleanPredicate().apply(TimeSeriesMetricExpression.TRUE).apply(ctx));
        testResult(true, UtilX.negateBooleanPredicate().apply(TimeSeriesMetricExpression.FALSE).apply(ctx));
    }

    @Test
    public void logicalAnd() {
        testResult(true, UtilX.logicalAnd().apply(TimeSeriesMetricExpression.TRUE, TimeSeriesMetricExpression.TRUE, TagMatchingClause.DEFAULT).apply(ctx));
        testResult(false, UtilX.logicalAnd().apply(TimeSeriesMetricExpression.FALSE, TimeSeriesMetricExpression.TRUE, TagMatchingClause.DEFAULT).apply(ctx));
        testResult(false, UtilX.logicalAnd().apply(TimeSeriesMetricExpression.TRUE, TimeSeriesMetricExpression.FALSE, TagMatchingClause.DEFAULT).apply(ctx));
        testResult(false, UtilX.logicalAnd().apply(TimeSeriesMetricExpression.FALSE, TimeSeriesMetricExpression.FALSE, TagMatchingClause.DEFAULT).apply(ctx));
    }

    @Test
    public void logicalOr() {
        testResult(true, UtilX.logicalOr().apply(TimeSeriesMetricExpression.TRUE, TimeSeriesMetricExpression.TRUE, TagMatchingClause.DEFAULT).apply(ctx));
        testResult(true, UtilX.logicalOr().apply(TimeSeriesMetricExpression.FALSE, TimeSeriesMetricExpression.TRUE, TagMatchingClause.DEFAULT).apply(ctx));
        testResult(true, UtilX.logicalOr().apply(TimeSeriesMetricExpression.TRUE, TimeSeriesMetricExpression.FALSE, TagMatchingClause.DEFAULT).apply(ctx));
        testResult(false, UtilX.logicalOr().apply(TimeSeriesMetricExpression.FALSE, TimeSeriesMetricExpression.FALSE, TagMatchingClause.DEFAULT).apply(ctx));
    }

    @Test
    public void falseIsZeroCoercion() {
        TimeSeriesMetricDeltaSet result = TimeSeriesMetricExpression.FALSE.apply(ctx);
        testCoercion(0, result);
    }

    @Test
    public void trueIsOneCoercion() {
        TimeSeriesMetricDeltaSet result = TimeSeriesMetricExpression.TRUE.apply(ctx);
        testCoercion(1, result);
    }

    @Test
    public void truePlus0() {
        testResult(1.0, UtilX.addition().apply(TimeSeriesMetricExpression.TRUE, constantExpression(0D), TagMatchingClause.DEFAULT).apply(ctx));
        testResult(1, UtilX.addition().apply(TimeSeriesMetricExpression.TRUE, constantExpression(0), TagMatchingClause.DEFAULT).apply(ctx));
        testResult(1, UtilX.addition().apply(TimeSeriesMetricExpression.TRUE, TimeSeriesMetricExpression.FALSE, TagMatchingClause.DEFAULT).apply(ctx));
    }

    @Test
    public void falsePlus0() {
        testResult(0.0, UtilX.addition().apply(TimeSeriesMetricExpression.FALSE, constantExpression(0D), TagMatchingClause.DEFAULT).apply(ctx));
        testResult(0, UtilX.addition().apply(TimeSeriesMetricExpression.FALSE, constantExpression(0), TagMatchingClause.DEFAULT).apply(ctx));
        testResult(0, UtilX.addition().apply(TimeSeriesMetricExpression.FALSE, TimeSeriesMetricExpression.FALSE, TagMatchingClause.DEFAULT).apply(ctx));
    }

    @Test
    public void numberLessThanPredicate() {
        TimeSeriesMetricDeltaSet result = UtilX.numberLessThanPredicate().apply(TimeSeriesMetricExpression.TRUE, TimeSeriesMetricExpression.FALSE, TagMatchingClause.DEFAULT).apply(ctx);
        testResult(1 < 0, result);
    }

    @Test
    public void numberLargerThanPredicate() {
        TimeSeriesMetricDeltaSet result = UtilX.numberLargerThanPredicate().apply(TimeSeriesMetricExpression.TRUE, TimeSeriesMetricExpression.FALSE, TagMatchingClause.DEFAULT).apply(ctx);
        testResult(1 > 0, result);
    }

    @Test
    public void numberLessEqualPredicate() {
        TimeSeriesMetricDeltaSet result = UtilX.numberLessEqualPredicate().apply(TimeSeriesMetricExpression.TRUE, TimeSeriesMetricExpression.FALSE, TagMatchingClause.DEFAULT).apply(ctx);
        testResult(1 <= 0, result);
    }

    @Test
    public void numberLargerEqualPredicate() {
        TimeSeriesMetricDeltaSet result = UtilX.numberLargerEqualPredicate().apply(TimeSeriesMetricExpression.TRUE, TimeSeriesMetricExpression.FALSE, TagMatchingClause.DEFAULT).apply(ctx);
        testResult(1 >= 0, result);
    }

    @Test
    public void numberEqualPredicate() {
        TimeSeriesMetricDeltaSet result = UtilX.equalPredicate().apply(TimeSeriesMetricExpression.TRUE, TimeSeriesMetricExpression.FALSE, TagMatchingClause.DEFAULT).apply(ctx);
        testResult(1 == 0, result);
    }

    @Test
    public void numberNotEqualPredicate() {
        TimeSeriesMetricDeltaSet result = UtilX.notEqualPredicate().apply(TimeSeriesMetricExpression.TRUE, TimeSeriesMetricExpression.FALSE, TagMatchingClause.DEFAULT).apply(ctx);
        testResult(1 != 0, result);
    }

    @Test
    public void equalityPredicateAcrossAllTypes() {
        final TimeSeriesMetricExpression none_expr = UtilX.equalPredicate().apply(Util.constantExpression(""), TimeSeriesMetricExpression.TRUE, TagMatchingClause.DEFAULT);
        final TimeSeriesMetricExpression bool_expr = TimeSeriesMetricExpression.TRUE;
        final TimeSeriesMetricExpression int_expr = Util.constantExpression(4L);
        final TimeSeriesMetricExpression flt_expr = Util.constantExpression(4D);
        final TimeSeriesMetricExpression str_expr = Util.constantExpression("foobar");

        testResult(true, UtilX.equalPredicate().apply(bool_expr, bool_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResult(false, UtilX.equalPredicate().apply(bool_expr, int_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResult(false, UtilX.equalPredicate().apply(bool_expr, flt_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResultEmpty(UtilX.equalPredicate().apply(bool_expr, str_expr, TagMatchingClause.DEFAULT).apply(ctx));

        testResult(false, UtilX.equalPredicate().apply(int_expr, bool_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResult(true, UtilX.equalPredicate().apply(int_expr, int_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResult(true, UtilX.equalPredicate().apply(int_expr, flt_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResultEmpty(UtilX.equalPredicate().apply(int_expr, str_expr, TagMatchingClause.DEFAULT).apply(ctx));

        testResult(false, UtilX.equalPredicate().apply(flt_expr, bool_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResult(true, UtilX.equalPredicate().apply(flt_expr, int_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResult(true, UtilX.equalPredicate().apply(flt_expr, flt_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResultEmpty(UtilX.equalPredicate().apply(flt_expr, str_expr, TagMatchingClause.DEFAULT).apply(ctx));

        testResultEmpty(UtilX.equalPredicate().apply(str_expr, bool_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResultEmpty(UtilX.equalPredicate().apply(str_expr, int_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResultEmpty(UtilX.equalPredicate().apply(str_expr, flt_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResult(true, UtilX.equalPredicate().apply(str_expr, str_expr, TagMatchingClause.DEFAULT).apply(ctx));

        testResultEmpty(UtilX.equalPredicate().apply(none_expr, bool_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResultEmpty(UtilX.equalPredicate().apply(none_expr, int_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResultEmpty(UtilX.equalPredicate().apply(none_expr, flt_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResultEmpty(UtilX.equalPredicate().apply(none_expr, str_expr, TagMatchingClause.DEFAULT).apply(ctx));

        testResultEmpty(UtilX.equalPredicate().apply(bool_expr, none_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResultEmpty(UtilX.equalPredicate().apply(int_expr, none_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResultEmpty(UtilX.equalPredicate().apply(flt_expr, none_expr, TagMatchingClause.DEFAULT).apply(ctx));
        testResultEmpty(UtilX.equalPredicate().apply(str_expr, none_expr, TagMatchingClause.DEFAULT).apply(ctx));
    }
}
