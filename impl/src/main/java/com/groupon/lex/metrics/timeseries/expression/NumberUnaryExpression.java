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
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.UNARY;
import static com.groupon.lex.metrics.timeseries.expression.Util.maybeBraces;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

/**
 *
 * @author ariane
 */
public class NumberUnaryExpression implements TimeSeriesMetricExpression {
    private final TimeSeriesMetricExpression expr_;
    private final Function<Double, Double> dbl_transform_;
    private final Function<Long, Long> long_transform_;
    private final String operand_string_;

    public NumberUnaryExpression(TimeSeriesMetricExpression expr, Function<Double, Double> dbl_transform, Function<Long, Long> long_transform, String operand_string) {
        expr_ = expr;
        dbl_transform_ = dbl_transform;
        long_transform_ = long_transform;
        operand_string_ = operand_string;
    }

    public String getOperandString() { return operand_string_; }
    @Override
    public Collection<TimeSeriesMetricExpression> getChildren() { return Collections.singleton(expr_); }

    private Number apply_1_(Number x) {
        if (x instanceof Float || x instanceof Double)
            return dbl_transform_.apply(x.doubleValue());
        else
            return long_transform_.apply(x.longValue());
    }

    private Optional<MetricValue> apply_(MetricValue x) {
        return x.value()
                .map(this::apply_1_)
                .map(MetricValue::fromNumberValue);
    }

    @Override
    public final TimeSeriesMetricDeltaSet apply(Context ctx) {
        return expr_.apply(ctx)
                .mapOptional(this::apply_);
    }

    @Override
    public int getPriority() {
        return UNARY;
    }

    @Override
    public StringBuilder configString() {
        return new StringBuilder()
                .append(getOperandString())
                .append(maybeBraces(getPriority(), expr_));
    }
}
