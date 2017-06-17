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
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.TriFunction;
import com.groupon.lex.metrics.timeseries.TagMatchingClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.ADDITION;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.LOGICAL_AND;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.LOGICAL_OR;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.MULTIPLICATION;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.SHIFT;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class UtilX {
    public static Function<TimeSeriesMetricExpression, TimeSeriesMetricExpression> negateBooleanPredicate() {
        return Util::negateBoolean;
    }

    public static Function<TimeSeriesMetricExpression, TimeSeriesMetricExpression> identityExpression() {
        return Util::identity;
    }

    public static Function<TimeSeriesMetricExpression, TimeSeriesMetricExpression> negateNumberExpression() {
        return Util::negateNumber;
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> addition() {
        return (x_expr, y_expr, matcher) -> new AbstractArithmaticExpression(x_expr, y_expr, "+", ADDITION, matcher) {
            @Override
            protected double expr(double x, double y) { return x + y; }
            @Override
            protected long expr(long x, long y) { return x + y; }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(double x, Histogram y) {
                return hist_expr(y, x);
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, double y) {
                return Optional.of(Any2.right(Histogram.add(x, y)));
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, Histogram y) {
                return Optional.of(Any2.right(Histogram.add(x, y)));
            }
        };
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> subtraction() {
        return (x_expr, y_expr, matcher) -> new AbstractArithmaticExpression(x_expr, y_expr, "-", ADDITION, matcher) {
            @Override
            protected double expr(double x, double y) { return x - y; }
            @Override
            protected long expr(long x, long y) { return x - y; }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(double x, Histogram y) {
                return Optional.of(Any2.right(y.modifyEventCounters((r, c) -> r.getWidth() * x - c)));
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, double y) {
                return Optional.of(Any2.right(Histogram.subtract(x, y)));
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, Histogram y) {
                return Optional.of(Any2.right(Histogram.subtract(x, y)));
            }
        };
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> leftShift() {
        return (x_expr, y_expr, matcher) -> new AbstractArithmaticExpression(x_expr, y_expr, "<<", SHIFT, matcher) {
            @Override
            protected double expr(double x, long y) { return Math.scalb(x, (int)y); }
            @Override
            protected double expr(double x, double y) { return x * Math.pow(2d, y); }
            @Override
            protected long expr(long x, long y) { return x << y; }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(double x, Histogram y) {
                return Optional.empty();
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, double y) {
                final double mul = Math.pow(2d, y);
                return Optional.of(Any2.right(Histogram.multiply(x, mul)));
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, Histogram y) {
                return Optional.empty();
            }
        };
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> rightShift() {
        return (x_expr, y_expr, matcher) -> new AbstractArithmaticExpression(x_expr, y_expr, ">>", SHIFT, matcher) {
            @Override
            protected double expr(double x, long y) { return Math.scalb(x, (int)-y); }
            @Override
            protected double expr(double x, double y) { return x * Math.pow(2d, -y); }
            @Override
            protected long expr(long x, long y) { return x >> y; }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(double x, Histogram y) {
                return Optional.empty();
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, double y) {
                final double mul = Math.pow(2d, -y);
                return Optional.of(Any2.right(Histogram.multiply(x, mul)));
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, Histogram y) {
                return Optional.empty();
            }
        };
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> multiply() {
        return (x_expr, y_expr, matcher) -> new AbstractArithmaticExpression(x_expr, y_expr, "*", MULTIPLICATION, matcher) {
            @Override
            protected double expr(double x, double y) { return x * y; }
            @Override
            protected long expr(long x, long y) { return x * y; }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(double x, Histogram y) {
                return hist_expr(y, x);
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, double y) {
                return Optional.of(Any2.right(Histogram.multiply(x, y)));
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, Histogram y) {
                return Optional.empty();
            }
        };
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> divide() {
        return (x_expr, y_expr, matcher) -> new AbstractArithmaticExpression(x_expr, y_expr, "/", MULTIPLICATION, matcher) {
            @Override
            protected double expr(double x, double y) { return x / y; }
            @Override
            protected long expr(long x, long y) { return x / y; }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(double x, Histogram y) {
                return Optional.empty();
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, double y) {
                return Optional.of(Any2.right(Histogram.divide(x, y)));
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, Histogram y) {
                return Optional.empty();
            }
        };
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> modulo() {
        return (x_expr, y_expr, matcher) -> new AbstractArithmaticExpression(x_expr, y_expr, "%", MULTIPLICATION, matcher) {
            @Override
            protected double expr(double x, double y) { return Math.IEEEremainder(x, y); }
            @Override
            protected long expr(long x, long y) { return x % y; }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(double x, Histogram y) {
                return Optional.empty();
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, double y) {
                return Optional.empty();
            }
            @Override
            protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, Histogram y) {
                return Optional.empty();
            }
        };
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> numberLessThanPredicate() {
        return Util::numberLessThanPredicate;
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> numberLargerThanPredicate() {
        return Util::numberLargerThanPredicate;
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> numberLessEqualPredicate() {
        return Util::numberLessEqualPredicate;
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> numberLargerEqualPredicate() {
        return Util::numberLargerEqualPredicate;
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> equalPredicate() {
        return Util::equalPredicate;
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> notEqualPredicate() {
        return Util::notEqualPredicate;
    }

    public static BiFunction<TimeSeriesMetricExpression, String, TimeSeriesMetricExpression> regexMatch() {
        return Util::regexMatch;
    }

    public static BiFunction<TimeSeriesMetricExpression, String, TimeSeriesMetricExpression> regexMismatch() {
        return Util::regexMismatch;
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> logicalAnd() {
        return (x_pred, y_pred, matcher) -> new LogicalBiPredicate(x_pred, y_pred, (x, y) -> x && y, "&&", LOGICAL_AND, matcher);
    }

    public static TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> logicalOr() {
        return (x_pred, y_pred, matcher) -> new LogicalBiPredicate(x_pred, y_pred, (x, y) -> x || y, "||", LOGICAL_OR, matcher);
    }

    public static Duration selectDuration(Duration... d) {
        if (d.length == 0) throw new IllegalArgumentException("No arguments");
        return Collections.max(Arrays.asList(d), Duration::compareTo);
    }
}
