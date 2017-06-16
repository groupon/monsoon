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

import com.groupon.lex.metrics.ConfigSupport;
import com.groupon.lex.metrics.expression.GroupExpression;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import java.util.Collection;
import java.util.Collections;
import static java.util.Objects.requireNonNull;
import java.util.stream.Stream;

/**
 * Tag extraction function.
 * @author ariane
 */
public class TagValueExpression implements TimeSeriesMetricExpression {
    private final Any2<TimeSeriesMetricExpression, GroupExpression> expr_;
    private final String tag_;

    public TagValueExpression(Any2<TimeSeriesMetricExpression, GroupExpression> expr, String tag) {
        expr_ = requireNonNull(expr);
        tag_ = requireNonNull(tag);
    }

    public TagValueExpression(TimeSeriesMetricExpression expr, String tag) {
        this(Any2.left(expr), tag);
    }

    public TagValueExpression(GroupExpression expr, String tag) {
        this(Any2.right(expr), tag);
    }

    @Override
    public Collection<TimeSeriesMetricExpression> getChildren() { return expr_.mapCombine(Collections::singleton, (ignored) -> Collections.<TimeSeriesMetricExpression>emptyList()); }

    @Override
    public TimeSeriesMetricDeltaSet apply(Context ctx) {
        return new TimeSeriesMetricDeltaSet(
                expr_.mapCombine(
                        expr -> expr.apply(ctx).streamAsMap()
                                .map(t -> t.getKey()),
                        group -> group.getTSDelta(ctx).asMap().values().stream()
                                .flatMap(Collection::stream)
                                .map(tsv -> tsv.getGroup())
                                .map(g -> g.getTags()))
                .map(tags -> SimpleMapEntry.create(tags, tags.getTag(tag_)))
                .map(tag_optval -> tag_optval.getValue().map(v -> SimpleMapEntry.create(tag_optval.getKey(), v)))
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty)));
    }

    @Override
    public int getPriority() {
        return BRACKETS;
    }

    @Override
    public StringBuilder configString() {
        return new StringBuilder()
                .append("tag(")
                .append(expr_.mapCombine(TimeSeriesMetricExpression::configString, GroupExpression::configString))
                .append(", ")
                .append(ConfigSupport.maybeQuoteIdentifier(tag_))
                .append(")");
    }
}
