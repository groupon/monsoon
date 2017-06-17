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

import com.groupon.lex.metrics.MetricValue;
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
public abstract class AbstractBiPredicate<X, Y> implements TimeSeriesMetricExpression {
    private final TimeSeriesMetricExpression x_arg_, y_arg_;
    private final Function<MetricValue, Optional<X>> x_impl_;
    private final Function<MetricValue, Optional<Y>> y_impl_;
    private final int priority_;
    private final TagMatchingClause matcher_;

    protected abstract Optional<Boolean> expr(X x, Y y);

    protected AbstractBiPredicate(
            TimeSeriesMetricExpression x_arg,
            TimeSeriesMetricExpression y_arg,
            Function<MetricValue, Optional<X>> x_impl,
            Function<MetricValue, Optional<Y>> y_impl,
            int priority,
            TagMatchingClause matcher) {
        x_arg_ = x_arg;
        y_arg_ = y_arg;
        x_impl_ = Objects.requireNonNull(x_impl);
        y_impl_ = Objects.requireNonNull(y_impl);
        priority_ = priority;
        matcher_ = Objects.requireNonNull(matcher);
    }

    public TimeSeriesMetricExpression getXArg() {
        return x_arg_;
    }

    public TimeSeriesMetricExpression getYArg() {
        return y_arg_;
    }

    @Override
    public Collection<TimeSeriesMetricExpression> getChildren() {
        return Arrays.asList(x_arg_, y_arg_);
    }

    private MetricValue apply_(MetricValue x, MetricValue y) {
        return pairwiseFlatMap(x_impl_.apply(x), y_impl_.apply(y), this::expr)
                .map(MetricValue::fromBoolean)
                .orElse(MetricValue.EMPTY);
    }

    @Override
    public TimeSeriesMetricDeltaSet apply(Context ctx) {
        final TimeSeriesMetricDeltaSet x_val = x_arg_.apply(ctx);
        final TimeSeriesMetricDeltaSet y_val = y_arg_.apply(ctx);
        return matcher_.apply(x_val, y_val, this::apply_);
    }

    protected final TagMatchingClause getMatcher() {
        return matcher_;
    }

    @Override
    public int getPriority() {
        return priority_;
    }
}
