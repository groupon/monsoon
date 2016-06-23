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

import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.expression.GroupExpression;
import com.groupon.lex.metrics.timeseries.PrintableExpression;
import com.groupon.lex.metrics.timeseries.TagMatchingClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.INEQUALITY;
import com.groupon.lex.metrics.transformers.NameResolver;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 *
 * @author ariane
 */
public class Util {
    private Util() {}  // Not meant to be instantiated.

    public static StringBuilder maybeBraces(int min_priority, PrintableExpression expr) {
        if (expr.getPriority() < min_priority) {
            return new StringBuilder()
                    .append("(")
                    .append(expr.configString())
                    .append(")");
        }
        return expr.configString();
    }

    public static <X, Y, R> Optional<R> pairwiseFlatMap(Optional<X> x, Optional<Y> y, BiFunction<? super X, ? super Y, Optional<R>> fn) {
        return x.flatMap((x_val) -> {
            return y.flatMap((y_val) -> {
                return fn.apply(x_val, y_val);
            });
        });
    }

    public static <X, Y, R> Optional<R> pairwiseMap(Optional<X> x, Optional<Y> y, BiFunction<? super X, ? super Y, R> fn) {
        return x.flatMap((x_val) -> {
            return y.map((y_val) -> {
                return fn.apply(x_val, y_val);
            });
        });
    }

    public static TimeSeriesMetricExpression numberLessThanPredicate(TimeSeriesMetricExpression x_val, TimeSeriesMetricExpression y_val, TagMatchingClause matcher) {
        return new NumberBiPredicate(x_val, y_val, (x, y) -> Double.compare(x, y) < 0, "<", INEQUALITY, matcher);
    }

    public static TimeSeriesMetricExpression numberLargerThanPredicate(TimeSeriesMetricExpression x_val, TimeSeriesMetricExpression y_val, TagMatchingClause matcher) {
        return new NumberBiPredicate(x_val, y_val, (x, y) -> Double.compare(x, y) > 0, ">", INEQUALITY, matcher);
    }

    public static TimeSeriesMetricExpression numberLessEqualPredicate(TimeSeriesMetricExpression x_val, TimeSeriesMetricExpression y_val, TagMatchingClause matcher) {
        return new NumberBiPredicate(x_val, y_val, (x, y) -> Double.compare(x, y) <= 0, "<=", INEQUALITY, matcher);
    }

    public static TimeSeriesMetricExpression numberLargerEqualPredicate(TimeSeriesMetricExpression x_val, TimeSeriesMetricExpression y_val, TagMatchingClause matcher) {
        return new NumberBiPredicate(x_val, y_val, (x, y) -> Double.compare(x, y) >= 0, ">=", INEQUALITY, matcher);
    }

    public static TimeSeriesMetricExpression equalPredicate(TimeSeriesMetricExpression x_val, TimeSeriesMetricExpression y_val, TagMatchingClause matcher) {
        return new EqualityPredicate(x_val, y_val, matcher);
    }

    public static TimeSeriesMetricExpression notEqualPredicate(TimeSeriesMetricExpression x_val, TimeSeriesMetricExpression y_val, TagMatchingClause matcher) {
        return new InequalityPredicate(x_val, y_val, matcher);
    }

    public static TimeSeriesMetricExpression selector(GroupExpression group, NameResolver metric) {
        return new MetricSelector(group, metric);
    }

    public static TimeSeriesMetricExpression timeSeriesExpression(GroupExpression group, MetricName metric) {
        return new TimeSeriesMetricSelector(group, metric);
    }

    public static TimeSeriesMetricExpression constantExpression(String value) {
        return new ConstantStringExpression(value);
    }

    public static TimeSeriesMetricExpression constantExpression(Number value) {
        return new ConstantNumberExpression(value);
    }

    public static TimeSeriesMetricExpression constantExpression(Histogram value) {
        return new ConstantHistogramExpression(value);
    }

    public static TimeSeriesMetricExpression negateNumber(TimeSeriesMetricExpression expr) {
        return new NumberUnaryExpression(expr, (x) -> -x, (x) -> -x, "-");
    }

    public static TimeSeriesMetricExpression negateBoolean(TimeSeriesMetricExpression x) {
        return new NegatePredicate(x);
    }

    public static TimeSeriesMetricExpression identity(TimeSeriesMetricExpression expr) {
        return expr;
    }

    public static TimeSeriesMetricExpression regexMatch(TimeSeriesMetricExpression expr, String regex) {
        return new RegexMatchPredicate(expr, regex);
    }

    public static TimeSeriesMetricExpression regexMismatch(TimeSeriesMetricExpression expr, String regex) {
        return new RegexMismatchPredicate(expr, regex);
    }
}
