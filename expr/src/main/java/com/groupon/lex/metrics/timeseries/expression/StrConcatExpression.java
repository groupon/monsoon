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
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import static java.util.Objects.requireNonNull;
import java.util.Optional;

/**
 * Concatenate one or more MetricValue instances, using their string representation.
 * @author ariane
 */
public class StrConcatExpression implements TimeSeriesMetricExpression {
    private static final TimeSeriesMetricDeltaSet INITIAL_ = new TimeSeriesMetricDeltaSet(MetricValue.fromStrValue(""));
    private final List<TimeSeriesMetricExpression> exprs_;
    private final TagMatchingClause matcher_;

    public StrConcatExpression(List<TimeSeriesMetricExpression> exprs, TagMatchingClause matcher) {
        exprs_ = unmodifiableList(new ArrayList<>(exprs));
        matcher_ = requireNonNull(matcher);
    }

    @Override
    public Collection<TimeSeriesMetricExpression> getChildren() { return exprs_; }

    private static Optional<MetricValue> concat_(MetricValue x, MetricValue y) {
        return Util.pairwiseMap(x.asString(), y.asString(), (r, s) -> r + s)
                .map(MetricValue::fromStrValue);
    }

    @Override
    public TimeSeriesMetricDeltaSet apply(Context ctx) {
        return exprs_.stream()
                .map(expr -> expr.apply(ctx))
                .reduce(INITIAL_, (r, s) -> matcher_.applyOptional(r, s, StrConcatExpression::concat_));
    }

    @Override
    public int getPriority() {
        return BRACKETS;
    }

    @Override
    public StringBuilder configString() {
        final StringBuilder result = new StringBuilder()
                .append("str(");

        boolean first = true;
        for (TimeSeriesMetricExpression expr : exprs_) {
            if (first)
                first = false;
            else
                result.append(", ");
            result.append(expr.configString());
        }
        result.append(')');

        result.append(Optional.of(matcher_.configString()).filter(sb -> sb.length() > 0).map(sb -> sb.insert(0, " ")).orElseGet(StringBuilder::new));

        return result;
    }
}
