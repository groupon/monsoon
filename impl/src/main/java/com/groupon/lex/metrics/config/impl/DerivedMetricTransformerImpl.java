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
package com.groupon.lex.metrics.config.impl;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.NameCache;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesTransformer;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.transformers.NameResolver;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.stream.Stream;

/**
 * Creates a new metric, by applying an expression.
 * @author ariane
 */
public class DerivedMetricTransformerImpl implements TimeSeriesTransformer {
    private final NameResolver group_;
    private final Map<NameResolver, TimeSeriesMetricExpression> mapping_;

    public final NameResolver getGroup() { return group_; }
    public final Map<NameResolver, TimeSeriesMetricExpression> getMapping() { return mapping_; }

    public DerivedMetricTransformerImpl(NameResolver group, NameResolver metric, TimeSeriesMetricExpression value_expr) {
        this(group, singletonMap(requireNonNull(metric), requireNonNull(value_expr)));
    }

    public DerivedMetricTransformerImpl(NameResolver group, Map<NameResolver, TimeSeriesMetricExpression> mapping) {
        group_ = requireNonNull(group);
        mapping_ = unmodifiableMap(new HashMap<>(requireNonNull(mapping)));
    }

    @Override
    public void transform(Context ctx) {
        final SimpleGroupPath group_name = getGroup().apply(ctx)
                .map(path -> NameCache.singleton.newSimpleGroupPath(path.getPath()))
                .orElseThrow(() -> new IllegalArgumentException("unable to resolve group name"));
        mapping_.entrySet().stream()
                .map(name_expr -> {
                    return name_expr.getKey().apply(ctx)
                            .map(path -> NameCache.singleton.newMetricName(path.getPath()))
                            .map(name -> SimpleMapEntry.create(name, name_expr.getValue()));
                })
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .forEach(metric_expr -> {
                    final MetricName metric = metric_expr.getKey();
                    final TimeSeriesMetricExpression expr = metric_expr.getValue();

                    expr
                            .apply(ctx)
                            .streamAsMap()
                            .forEach((Entry<Tags, MetricValue> entry) -> {
                                final GroupName grp = NameCache.singleton.newGroupName(group_name, entry.getKey());
                                final MetricValue tsdelta = entry.getValue();
                                ctx.getTSData().getCurrentCollection()
                                        .addMetric(grp, metric, tsdelta);
                            });
                });
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 61 * hash + Objects.hashCode(this.group_);
        hash = 61 * hash + Objects.hashCode(this.mapping_);
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
        final DerivedMetricTransformerImpl other = (DerivedMetricTransformerImpl) obj;
        if (!Objects.equals(this.group_, other.group_)) {
            return false;
        }
        if (!Objects.equals(this.mapping_, other.mapping_)) {
            return false;
        }
        return true;
    }

    @Override
    public ExpressionLookBack getLookBack() {
        return ExpressionLookBack.EMPTY.andThen(mapping_.values().stream()
                .map(TimeSeriesMetricExpression::getLookBack));
    }
}
