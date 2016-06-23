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
package com.groupon.lex.metrics.config;

import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.NameCache;
import com.groupon.lex.metrics.expression.GroupExpression;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesTransformer;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.transformers.NameResolver;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public class ResolvedConstantStatement implements RuleStatement {
    private final GroupExpression group_;
    private final Map<NameResolver, MetricValue> metrics_;

    public ResolvedConstantStatement(GroupExpression group, NameResolver metric, MetricValue value) {
        group_ = Objects.requireNonNull(group);
        metrics_ = singletonMap(Objects.requireNonNull(metric), Objects.requireNonNull(value));
    }

    public ResolvedConstantStatement(GroupExpression group, Map<NameResolver, MetricValue> metrics) {
        group_ = Objects.requireNonNull(group);
        metrics_ = unmodifiableMap(new HashMap<NameResolver, MetricValue>(metrics));
    }

    public Map<NameResolver, MetricValue> getMetrics() { return metrics_; }

    private TimeSeriesCollection transform_(Context ctx) {
        final TimeSeriesCollection ts_data = ctx.getTSData().getCurrentCollection();
        final Map<MetricName, MetricValue> metrics = metrics_.entrySet().stream()
                .map(name_value -> {
                    return name_value.getKey().apply(ctx).map(name -> SimpleMapEntry.create(name, name_value.getValue()));
                })
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .map(path_value -> SimpleMapEntry.create(NameCache.singleton.newMetricName(path_value.getKey().getPath()), path_value.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        group_.getTSDelta(ctx).stream()
                .map(TimeSeriesValue::getGroup)
                .forEach((grp) -> ts_data.addMetrics(grp, metrics));
        return ts_data;
    }

    @Override
    public TimeSeriesTransformer get() {
        return new TimeSeriesTransformer() {
            @Override
            public void transform(Context ctx) {
                transform_(ctx);
            }

            @Override
            public ExpressionLookBack getLookBack() {
                return ExpressionLookBack.EMPTY;
            }
        };
    }

    @Override
    public StringBuilder configString() {
        StringBuilder result = new StringBuilder()
                .append("constant ")
                .append(group_.configString());

        if (metrics_.size() == 1) {
            metrics_.entrySet()
                    .forEach((metric) -> result
                            .append(' ')
                            .append(metric.getKey().configString())
                            .append(' ')
                            .append(metric.getValue()));
            result.append(";\n");
        } else {
            Optional<StringBuilder> body = metrics_.entrySet().stream()
                    .sorted(Comparator.comparing(metric -> metric.getKey().configString().toString()))
                    .map((metric) -> new StringBuilder()
                            .append(metric.getKey().configString())
                            .append(' ')
                            .append(metric.getValue())
                            .append(';'))
                    .reduce((x, y) -> x.append("\n  ").append(y))
                    .map((s) -> s.insert(0, "\n  "));
            result
                    .append(" {")
                    .append(body.orElseGet(StringBuilder::new))
                    .append("\n}\n");
        }

        return result;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 37 * hash + Objects.hashCode(this.group_);
        hash = 37 * hash + Objects.hashCode(this.metrics_);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ResolvedConstantStatement other = (ResolvedConstantStatement) obj;
        if (!Objects.equals(this.group_, other.group_)) {
            return false;
        }
        if (!Objects.equals(this.metrics_, other.metrics_)) {
            return false;
        }
        return true;
    }
}
