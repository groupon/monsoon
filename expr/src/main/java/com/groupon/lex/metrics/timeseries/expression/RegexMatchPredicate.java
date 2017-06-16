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
import static com.groupon.lex.metrics.ConfigSupport.quotedString;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.EQUALITY;
import static com.groupon.lex.metrics.timeseries.expression.Util.maybeBraces;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 *
 * @author ariane
 */
public class RegexMatchPredicate implements TimeSeriesMetricExpression {
    private final Predicate<String> match_;
    private final TimeSeriesMetricExpression expr_;
    private final String regex_;

    public RegexMatchPredicate(TimeSeriesMetricExpression expr, String regex) {
        match_ = Pattern.compile(regex).asPredicate();
        expr_ = expr;
        regex_ = regex;
    }

    @Override
    public Collection<TimeSeriesMetricExpression> getChildren() { return Collections.singleton(expr_); }

    private Optional<MetricValue> apply_(MetricValue val) {
        return val.stringValue()
                .map(match_::test)
                .map(MetricValue::fromBoolean);
    }

    @Override
    public TimeSeriesMetricDeltaSet apply(Context ctx) {
        return expr_.apply(ctx)
                .mapOptional(this::apply_);
    }

    @Override
    public int getPriority() {
        return EQUALITY;
    }

    @Override
    public StringBuilder configString() {
        return new StringBuilder()
                .append(maybeBraces(getPriority(), expr_))
                .append(" =~ ")
                .append(quotedString(regex_));
    }
}
