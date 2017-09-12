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

import com.groupon.lex.metrics.MetricMatcher;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.PathMatcher;
import com.groupon.lex.metrics.expression.GroupExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricFilter;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 *
 * @author ariane
 */
public class TimeSeriesMetricSelector implements TimeSeriesMetricExpression {
    private final GroupExpression group_;
    private final MetricName metric_;

    public TimeSeriesMetricSelector(GroupExpression group, MetricName metric) {
        group_ = group;
        metric_ = metric;
    }

    @Override
    public Collection<TimeSeriesMetricExpression> getChildren() { return Collections.EMPTY_LIST; }

    @Override
    public TimeSeriesMetricFilter getNameFilter() {
        final PathMatcher metricMatcher = new PathMatcher(metric_.getPath().stream()
                .map(PathMatcher.LiteralNameMatch::new)
                .collect(Collectors.toList()));
        return new TimeSeriesMetricFilter()
                .withMetric(new MetricMatcher(group_.getPathMatcher(), metricMatcher));
    }

    public GroupExpression getGroup() { return group_; }
    public MetricName getMetric() { return metric_; }

    @Override
    public TimeSeriesMetricDeltaSet apply(Context ctx) {
        return group_.getTSDelta(ctx).findMetric(metric_);
    }

    @Override
    public int getPriority() {
        return BRACKETS;
    }

    @Override
    public StringBuilder configString() {
        return new StringBuilder()
                .append(getGroup().configString())
                .append(" ")
                .append(getMetric().configString());
    }
}
