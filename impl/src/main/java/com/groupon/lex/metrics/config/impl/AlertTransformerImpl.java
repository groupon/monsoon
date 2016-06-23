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
import com.groupon.lex.metrics.NameCache;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.config.AlertStatement;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TagAggregationClause;
import com.groupon.lex.metrics.timeseries.TagMatchingClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesTransformer;
import com.groupon.lex.metrics.timeseries.expression.Context;
import static java.util.Collections.EMPTY_LIST;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public class AlertTransformerImpl implements TimeSeriesTransformer {
    private final AlertStatement alert_defn_;

    public AlertTransformerImpl(AlertStatement alert_defn) {
        alert_defn_ = alert_defn;
    }

    private Map<String, Any2<TimeSeriesMetricDeltaSet, List<TimeSeriesMetricDeltaSet>>> calculate_attributes_(Context ctx) {
        return alert_defn_.getAttributes().entrySet().stream()
                .map(attr -> {
                    final String key = attr.getKey();
                    final Any2<TimeSeriesMetricDeltaSet, List<TimeSeriesMetricDeltaSet>> value = attr.getValue().map(
                            expr -> expr.apply(ctx),
                            expr_set -> expr_set.stream().map(expr -> expr.apply(ctx)).collect(Collectors.toList()));
                    return SimpleMapEntry.create(key, value);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<String, Any2<MetricValue, List<MetricValue>>> lookup_attributes_(Tags tags, Map<String, Any2<TimeSeriesMetricDeltaSet, List<TimeSeriesMetricDeltaSet>>> values) {
        final TagMatchingClause matcher = TagMatchingClause.by(tags.asMap().keySet(), false);
        final TagAggregationClause aggregator = TagAggregationClause.by(tags.asMap().keySet(), false);

        return values.entrySet().stream()
                .map(value -> {
                    final Any2<MetricValue, List<MetricValue>> rhs = value.getValue()
                            .map(
                                    tsvs -> {
                                        return matcher.apply(
                                                Stream.of(tags),
                                                tsvs.streamAsMap(tags),
                                                Function.identity(),
                                                Map.Entry::getKey,
                                                (t, tsv) -> tsv.getValue());
                                    },
                                    tsvs_list -> {
                                        return aggregator.apply(
                                                tsvs_list.stream().flatMap(tsvs -> tsvs.streamAsMap(tags)),
                                                Map.Entry::getKey,
                                                Map.Entry::getValue);
                                    })
                            .map(
                                    tsvs -> tsvs.values().stream().findAny().orElse(MetricValue.EMPTY),
                                    tsvs -> Optional.ofNullable(tsvs.get(tags)).orElse(EMPTY_LIST));
                    return SimpleMapEntry.create(value.getKey(), rhs);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Stream<Alert> create_alert_(Context ctx) {
        final TimeSeriesCollectionPair ts_data = ctx.getTSData();
        final Map<String, Any2<TimeSeriesMetricDeltaSet, List<TimeSeriesMetricDeltaSet>>> attr_map = calculate_attributes_(ctx);
        return alert_defn_.getName().apply(ctx)
                .map(path -> NameCache.singleton.newSimpleGroupPath(path.getPath()))
                .map((name) -> {
                    return alert_defn_.getPredicate().apply(ctx)
                            .streamAsMap()
                            .map((tag_val) -> SimpleMapEntry.create(tag_val.getKey(), tag_val.getValue().asBool()))
                            .map((tag_bool) -> {
                                final GroupName alert_name = NameCache.singleton.newGroupName(name, tag_bool.getKey());
                                final Optional<Boolean> triggering = tag_bool.getValue();
                                Map<String, Any2<MetricValue, List<MetricValue>>> attrs = lookup_attributes_(alert_name.getTags(), attr_map);
                                return new Alert(ts_data.getCurrentCollection().getTimestamp(), alert_name, alert_defn_::configString, triggering, alert_defn_.getFireDuration(), alert_defn_.getMessage(), attrs);
                            });
                })
                .orElseGet(Stream::empty);
    }

    @Override
    public void transform(Context ctx) {
        create_alert_(ctx).forEach(ctx.getAlertManager()::accept);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 67 * hash + Objects.hashCode(this.alert_defn_);
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
        final AlertTransformerImpl other = (AlertTransformerImpl) obj;
        if (!Objects.equals(this.alert_defn_, other.alert_defn_)) {
            return false;
        }
        return true;
    }

    @Override
    public ExpressionLookBack getLookBack() {
        return ExpressionLookBack.EMPTY.andThen(Stream.concat(
                Stream.of(alert_defn_.getPredicate().getLookBack()),
                alert_defn_.getAttributes().values().stream()
                        .flatMap(expr_or_list -> expr_or_list.mapCombine(expr -> Stream.of(expr.getLookBack()), list -> list.stream().map(expr -> expr.getLookBack())))
        ));
    }
}
