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

import static com.groupon.lex.metrics.ConfigSupport.quotedString;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.lib.StringTemplate;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * String rewriting, using a regexp and a string template.
 * @author ariane
 */
public class RegexpExpression implements TimeSeriesMetricExpression {
    private final TimeSeriesMetricExpression expr_;
    private final Pattern pattern_;
    private final StringTemplate template_;

    public RegexpExpression(TimeSeriesMetricExpression expr, Pattern pattern, StringTemplate template) {
        expr_ = requireNonNull(expr);
        pattern_ = requireNonNull(pattern);
        template_ = requireNonNull(template);
    }

    @Override
    public Collection<TimeSeriesMetricExpression> getChildren() { return Collections.singleton(expr_); }

    public RegexpExpression(TimeSeriesMetricExpression expr, String regexp, String template) {
        this(expr, Pattern.compile(regexp), StringTemplate.fromString(template));
    }

    @Override
    public TimeSeriesMetricDeltaSet apply(Context ctx) {
        return expr_.apply(ctx).mapOptional(this::rewrite_);
    }

    @Override
    public int getPriority() {
        return BRACKETS;
    }

    @Override
    public StringBuilder configString() {
        return new StringBuilder()
                .append("regexp(")
                .append(expr_.configString())
                .append(", ")
                .append(quotedString(pattern_.pattern()))
                .append(", ")
                .append(template_.configString())
                .append(')');
    }

    /** Convert a regexp match to a map of matcher arguments. */
    private Map<Any2<String, Integer>, String> resolve_template_args_(Matcher matcher) {
        return template_.getArguments().stream()
                .collect(Collectors.toMap(arg -> arg, arg -> arg.mapCombine(matcher::group, matcher::group)));
    }

    /**
     * Rewrite a string metric, according to matcher and template.
     * Strings that don't match the regexp are omitted.
     */
    private Optional<MetricValue> rewrite_(MetricValue v) {
        return v.asString()
                .map(pattern_::matcher)
                .filter(matcher -> matcher.find())
                .map(this::resolve_template_args_)
                .map(template_::apply)
                .map(MetricValue::fromStrValue);
    }
}
