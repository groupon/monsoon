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
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.expression.GroupExpression;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesTransformer;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import com.groupon.lex.metrics.timeseries.expression.Context;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public class SetTag implements TimeSeriesTransformer {
    private static final Logger LOG = Logger.getLogger(SetTag.class.getName());
    private final GroupExpression group_;
    private final Map<String, TimeSeriesMetricExpression> tags_;

    public SetTag(GroupExpression group, Map<String, TimeSeriesMetricExpression> tags) {
        group_ = requireNonNull(group);
        tags_ = requireNonNull(tags);
    }

    public final GroupExpression getGroup() { return group_; }

    private static GroupName calculate_new_name_(GroupName oldname, Map<String, MetricValue> tagged_values) {
        return GroupName.valueOf(oldname.getPath(),
                Stream.concat(
                        oldname.getTags().stream().filter((Map.Entry<String, MetricValue> entry) -> !tagged_values.keySet().contains(entry.getKey())),
                        tagged_values.entrySet().stream()));
    }

    @Override
    public void transform(Context ctx) {
        final TimeSeriesValueSet group_name = getGroup().getTSDelta(ctx);
        final List<GroupName> origin = group_name.stream()
                .map(TimeSeriesValue::getGroup)
                .collect(Collectors.toList());
        LOG.log(Level.FINER, "origin = {0}", origin);

        // Resolve each tag expression.
        final Map<String, TimeSeriesMetricDeltaSet> values = tags_.entrySet().stream()
                .map(tag -> SimpleMapEntry.create(tag.getKey(), tag.getValue().apply(ctx)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // Extract and save all the scalars.
        final List<Map.Entry<String, MetricValue>> scalars = values.entrySet().stream()
                .filter(value -> value.getValue().isScalar())
                .map(named_scalar -> {
                    return named_scalar.getValue().asScalar().map(scalar -> SimpleMapEntry.create(named_scalar.getKey(), scalar));
                })
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .filter(entry -> !entry.getValue().histogram().isPresent())  // Skip any histograms.
                .collect(Collectors.toList());
        LOG.log(Level.FINER, "scalars = {0}", scalars);

        // Extract and save all the vectors, grouped by tag set.
        final Map<Tags, List<Map.Entry<String, MetricValue>>> vectors = values.entrySet().stream()
                .filter(value -> value.getValue().isVector())
                .flatMap(value -> {
                    return value.getValue().asVector()
                            .orElseGet(Collections::<Tags, MetricValue>emptyMap)
                            .entrySet().stream()
                            .filter(entry -> !entry.getValue().histogram().isPresent())  // Skip any histograms.
                            .map(tagged_value -> SimpleMapEntry.create(tagged_value.getKey(), SimpleMapEntry.create(value.getKey(), tagged_value.getValue())));
                })
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

        // Create a mapping for each rename...
        final List<Map.Entry<GroupName, GroupName>> renames = origin.stream()
                .map(old_name -> {
                    Map<String, MetricValue> new_tags = new HashMap<>();
                    vectors.getOrDefault(old_name.getTags(), Collections.<Map.Entry<String, MetricValue>>emptyList())
                            .forEach(entry -> new_tags.put(entry.getKey(), entry.getValue()));
                    scalars
                            .forEach(entry -> new_tags.put(entry.getKey(), entry.getValue()));
                    return SimpleMapEntry.create(old_name, calculate_new_name_(old_name, new_tags));
                })
                .filter(entry -> !entry.getKey().equals(entry.getValue()))  // Filter out noop renames.
                .collect(Collectors.toList());

        // Run rename cycle.
        renames
                .forEach(entry -> {
                    final GroupName oldname = entry.getKey();
                    final GroupName newname = entry.getValue();
                    ctx.getTSData().getCurrentCollection().renameGroup(oldname, newname);
                });
    }

    @Override
    public ExpressionLookBack getLookBack() {
        return ExpressionLookBack.EMPTY.andThen(tags_.values().stream().map(TimeSeriesMetricExpression::getLookBack));
    }
}
