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
import com.groupon.lex.metrics.MutableTimeSeriesCollectionPair;
import com.groupon.lex.metrics.Path;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesTransformer;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.transformers.NameResolver;
import java.util.Collection;
import static java.util.Collections.EMPTY_MAP;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

/**
 * Creates a new metric, by applying an expression.
 * @author ariane
 */
@Value
public class DerivedMetricTransformerImpl implements TimeSeriesTransformer {
    @NonNull
    private final NameResolver group;
    @NonNull
    private final Map<String, TimeSeriesMetricExpression> tags;
    @NonNull
    private final Map<NameResolver, TimeSeriesMetricExpression> mapping;

    private static <T, U> Stream<Map.Entry<U, TimeSeriesMetricDeltaSet>> resolveMapping(Context ctx, Map<T, TimeSeriesMetricExpression> mapping, Function<? super T, Optional<? extends U>> keyMapper) {
        return mapping.entrySet().stream()
                .map(t_expr -> {
                    return keyMapper.apply(t_expr.getKey())
                            .map(key -> SimpleMapEntry.create(key, t_expr.getValue()));
                })
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .map(u_expr -> {
                    return SimpleMapEntry.create(u_expr.getKey(), u_expr.getValue().apply(ctx));
                });
    }

    private static Optional<MetricName> resolveMetricName(Context ctx, NameResolver resolver) {
        return resolver.apply(ctx)
                .map(Path::getPath)
                .map(MetricName::valueOf);
    }

    @Override
    public void transform(Context ctx) {
        final SimpleGroupPath group_name = getGroup().apply(ctx)
                .map(path -> SimpleGroupPath.valueOf(path.getPath()))
                .orElseThrow(() -> new IllegalArgumentException("unable to resolve group name"));
        final TagData tagData = new TagData(ctx, tags);

        resolveMapping(ctx, mapping, name -> resolveMetricName(ctx, name))
                .forEach(metric_expr -> {
                    final MetricName metric = metric_expr.getKey();
                    metric_expr.getValue().streamAsMap()
                            .forEach((Map.Entry<Tags, MetricValue> resolvedMetric) -> addMetricMappingToCtx(ctx, group_name, metric, tagData, resolvedMetric));
                });
    }

    @Override
    public ExpressionLookBack getLookBack() {
        return ExpressionLookBack.EMPTY.andThen(Stream.of(tags, mapping)
                .map(Map::values)
                .flatMap(Collection::stream)
                .map(TimeSeriesMetricExpression::getLookBack));
    }

    private static void addMetricMappingToCtx(Context<MutableTimeSeriesCollectionPair> ctx, SimpleGroupPath groupPath, MetricName metric, TagData tagData, Map.Entry<Tags, MetricValue> resolvedMetric) {
        final GroupName group;
        if (tagData.isEmpty()) {
            group = GroupName.valueOf(groupPath, resolvedMetric.getKey());
        } else {
            final Map<String, MetricValue> tagMap = new HashMap<>();
            tagMap.putAll(resolvedMetric.getKey().asMap());
            tagData.getExtraTagsForTags(resolvedMetric.getKey())
                    .forEach((tagValue) -> tagMap.put(tagValue.getKey(), tagValue.getValue()));
            group = GroupName.valueOf(groupPath, tagMap);
        }

        final MetricValue metricValue = resolvedMetric.getValue();
        ctx.getTSData().getCurrentCollection()
                .addMetric(group, metric, metricValue);
    }

    @Getter
    private static class TagData {
        private final Map<String, MetricValue> scalarTags = new HashMap<>();
        private final Map<Tags, Map<String, MetricValue>> vectorTags = new HashMap<>();

        /** Initialize TagData from tag expressions. */
        public TagData(Context ctx, Map<String, TimeSeriesMetricExpression> tagExpressions) {
            resolveMapping(ctx, tagExpressions, Optional::of)
                    .forEach(this::addTag);
        }

        public boolean isEmpty() {
            return scalarTags.isEmpty() && vectorTags.isEmpty();
        }

        /** Register the resolution of a tag expression. */
        public void addTag(Map.Entry<String, TimeSeriesMetricDeltaSet> tag) {
            tag.getValue().asScalar()
                    .ifPresent(scalar -> scalarTags.put(tag.getKey(), scalar));
            tag.getValue().asVector()
                    .ifPresent(vector -> {
                        vector.forEach((tags, value) -> {
                            vectorTags.computeIfAbsent(tags, (ignored) -> new HashMap<>())
                                    .put(tag.getKey(), value);
                        });
                    });
        }

        /** Returns overridden/added tags for a metric with the given resolved tag set. */
        public Stream<Map.Entry<String, MetricValue>> getExtraTagsForTags(Tags tags) {
            return Stream.concat(
                    scalarTags.entrySet().stream(),
                    vectorTags.getOrDefault(tags, EMPTY_MAP).entrySet().stream());
        }
    }
}
