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
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.timeseries.TagMatchingClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Util.pairwiseFlatMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 *
 * @author ariane
 */
public abstract class AbstractBiExpression<X, Y, R extends Number> implements TimeSeriesMetricExpression {
    private final TimeSeriesMetricExpression x_arg_, y_arg_;
    private final Function<MetricValue, Optional<Any2<? extends X, Histogram>>> x_impl_;
    private final Function<MetricValue, Optional<Any2<? extends Y, Histogram>>> y_impl_;
    private final TagMatchingClause matcher_;

    protected abstract Optional<Any2<? extends R, Histogram>> expr(X x, Y y);

    protected abstract Optional<Any2<? extends R, Histogram>> expr(X x, Histogram y);

    protected abstract Optional<Any2<? extends R, Histogram>> expr(Histogram x, Y y);

    protected abstract Optional<Any2<? extends R, Histogram>> expr(Histogram x, Histogram y);

    private Optional<Any2<? extends R, Histogram>> expr_(Any2<? extends X, Histogram> x, Any2<? extends Y, Histogram> y) {
        return x.mapCombine(
                x_number -> y.mapCombine(
                        y_number -> expr(x_number, y_number),
                        y_histogram -> expr(x_number, y_histogram)
                ),
                x_histogram -> y.mapCombine(
                        y_number -> expr(x_histogram, y_number),
                        y_histogram -> expr(x_histogram, y_histogram)
                )
        );
    }

    @Override
    public Collection<TimeSeriesMetricExpression> getChildren() {
        return Arrays.asList(x_arg_, y_arg_);
    }

    protected AbstractBiExpression(
            TimeSeriesMetricExpression x_arg,
            TimeSeriesMetricExpression y_arg,
            Function<MetricValue, Optional<Any2<? extends X, Histogram>>> x_impl,
            Function<MetricValue, Optional<Any2<? extends Y, Histogram>>> y_impl,
            TagMatchingClause matcher) {
        x_arg_ = Objects.requireNonNull(x_arg);
        y_arg_ = Objects.requireNonNull(y_arg);
        x_impl_ = Objects.requireNonNull(x_impl);
        y_impl_ = Objects.requireNonNull(y_impl);
        matcher_ = Objects.requireNonNull(matcher);
    }

    protected TimeSeriesMetricExpression getXArg() {
        return x_arg_;
    }

    protected TimeSeriesMetricExpression getYArg() {
        return y_arg_;
    }

    private MetricValue apply_(MetricValue x, MetricValue y) {
        return pairwiseFlatMap(x_impl_.apply(x), y_impl_.apply(y), this::expr_)
                .map(num_or_hist -> num_or_hist.mapCombine(MetricValue::fromNumberValue, MetricValue::fromHistValue))
                .orElse(MetricValue.EMPTY);
    }

    protected final TagMatchingClause getMatcher() {
        return matcher_;
    }

    @Override
    public TimeSeriesMetricDeltaSet apply(Context ctx) {
        final TimeSeriesMetricDeltaSet x_val = x_arg_.apply(ctx);
        final TimeSeriesMetricDeltaSet y_val = y_arg_.apply(ctx);
        return matcher_.apply(x_val, y_val, this::apply_);
    }
}
