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

import com.groupon.lex.metrics.MetricMatcher;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.PathMatcher;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.config.MatchStatement;
import com.groupon.lex.metrics.config.MatchStatement.IdentifierPair;
import com.groupon.lex.metrics.lib.MemoidOne;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TimeSeriesTransformer;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.timeseries.expression.MutableContext;
import java.util.Collection;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public class MatchTransformerImpl implements TimeSeriesTransformer {
    private static final Logger LOG = Logger.getLogger(MatchTransformerImpl.class.getName());
    private final DecoratorMap decorator_;
    private final Optional<MatchStatement.LookBackExposingPredicate> where_clause_;
    private final Collection<TimeSeriesTransformer> rules_;

    public MatchTransformerImpl(Map<String, PathMatcher> groupNameMatchers,
            Map<IdentifierPair, MetricMatcher> metricNameMatchers,
            Optional<MatchStatement.LookBackExposingPredicate> where_clause,
            Stream<TimeSeriesTransformer> rules) {
        decorator_ = concat(group_map_(groupNameMatchers), metric_map_(metricNameMatchers));
        where_clause_ = Objects.requireNonNull(where_clause);
        rules_ = unmodifiableList(rules.collect(Collectors.toList()));
    }

    private static interface DecoratorMap {
        public List<Consumer<MutableContext>> map(List<Consumer<MutableContext>> in, Context ctx);
    }

    /** Create a decorator map from a mapping of GroupMatchers. */
    private static DecoratorMap group_map_(Map<String, PathMatcher> paths) {
        if (paths.isEmpty()) return concat();

        // Provide caching of group name lookups.
        final Function<Collection<SimpleGroupPath>, List<Map.Entry<String, List<SimpleGroupPath>>>> path_mapping =
                new MemoidOne<>((Collection<SimpleGroupPath> sgp) -> {
                    return paths.entrySet().stream()
                            .map(pathmatcher -> {
                                final List<SimpleGroupPath> filtered_sgp = sgp.stream()
                                        .filter(p -> pathmatcher.getValue().match(p.getPath()))
                                        .collect(Collectors.toList());
                                return SimpleMapEntry.create(pathmatcher.getKey(), filtered_sgp);
                            })
                            .collect(Collectors.toList());
                });

        return new DecoratorMap() {
            private List<Consumer<MutableContext>> map_(List<Consumer<MutableContext>> in, Iterator<Map.Entry<String, List<TimeSeriesValue>>> data_iter) {
                while (data_iter.hasNext()) {
                    final Map.Entry<String, List<TimeSeriesValue>> next = data_iter.next();
                    final String identifier = next.getKey();
                    LOG.log(Level.FINE, "emitting consumers for {0}", identifier);
                    final List<Consumer<MutableContext>> reference_to_in = in;
                    in = next.getValue().stream()
                            .map(tsv -> {
                                final Consumer<MutableContext> decorator = (MutableContext out) -> out.putGroupAliasByName(identifier, tsv::getGroup);
                                return decorator;
                            })
                            .flatMap((Consumer<MutableContext> decorator) -> reference_to_in.stream().map(decorator::andThen))
                            .collect(Collectors.toList());
                }
                return in;
            }

            @Override
            public List<Consumer<MutableContext>> map(List<Consumer<MutableContext>> in, Context ctx) {
                final Stream<Map.Entry<String, List<TimeSeriesValue>>> data = path_mapping.apply(new HashSet<>(ctx.getTSData().getCurrentCollection().getGroupPaths())).stream()
                        .map(ident_path -> {
                            final List<TimeSeriesValue> tsvs = ident_path.getValue().stream()
                                    .map(ctx.getTSData().getCurrentCollection()::getTSValue)
                                    .flatMap(TimeSeriesValueSet::stream)
                                    .collect(Collectors.toList());
                            return SimpleMapEntry.create(ident_path.getKey(), tsvs);
                        });

                return map_(in, data.iterator());
            }
        };
    }

    /** Create a decorator map from a mapping of metric matchers. */
    private static DecoratorMap metric_map_(Map<IdentifierPair, MetricMatcher> matchers) {
        return new DecoratorMap() {
            private List<Consumer<MutableContext>> map_(Context ctx, List<Consumer<MutableContext>> in, Iterator<Map.Entry<IdentifierPair, List<Map.Entry<MetricMatcher.MatchedName, MetricValue>>>> data_iter) {
                while (data_iter.hasNext()) {
                    final Map.Entry<IdentifierPair, List<Map.Entry<MetricMatcher.MatchedName, MetricValue>>> next = data_iter.next();
                    final String group_ident = next.getKey().getGroup();
                    final String metric_ident = next.getKey().getMetric();
                    LOG.log(Level.FINE, "emitting consumers for {0}, {1}", new Object[]{group_ident, metric_ident});
                    final List<Consumer<MutableContext>> reference_to_in = in;
                    in = next.getValue().stream()
                            .map(Map.Entry::getKey)
                            .map(matchname -> {
                                final TimeSeriesValue tsv = ctx.getTSData().getCurrentCollection().get(matchname.getGroup()).orElseThrow(() -> new IllegalStateException("resolved group does not exist"));
                                final Consumer<MutableContext> decorator = (MutableContext out) -> {
                                    out.putGroupAliasByName(group_ident, tsv::getGroup);
                                    out.putMetricAliasByName(metric_ident, tsv::getGroup, matchname::getMetric);
                                };
                                return decorator;
                            })
                            .flatMap((Consumer<MutableContext> decorator) -> reference_to_in.stream().map(decorator::andThen))
                            .collect(Collectors.toList());
                }
                return in;
            }

            @Override
            public List<Consumer<MutableContext>> map(List<Consumer<MutableContext>> in, Context ctx) {
                final Stream<Map.Entry<IdentifierPair, List<Map.Entry<MetricMatcher.MatchedName, MetricValue>>>> data = matchers.entrySet().stream()
                        .map(ident_matcher -> SimpleMapEntry.create(ident_matcher.getKey(), ident_matcher.getValue().filter(ctx).collect(Collectors.toList())));

                return map_(ctx, in, data.iterator());
            }
        };
    }

    /** Create a decorator map, by chaining zero or more decorator maps. */
    private static DecoratorMap concat(DecoratorMap... d_maps) {
        return (List<Consumer<MutableContext>> in, Context ctx) -> {
            for (DecoratorMap d_map : d_maps) {
                in = d_map.map(in, ctx);
            }
            return in;
        };
    }

    private void play_rules_(Consumer<MutableContext> decorator, Context parent) {
        final MutableContext ctx = new MutableContext(parent);
        decorator.accept(ctx);
        if (where_clause_.map((pred) -> pred.test(ctx)).orElse(Boolean.TRUE))
            rules_.forEach(rule -> rule.transform(ctx));
    }

    @Override
    public void transform(Context ctx) {
        decorator_
                .map(singletonList((MutableContext mctx) -> {}), ctx)
                .forEach(decorator -> play_rules_(decorator, ctx));
    }

    public ExpressionLookBack getLookBack() {
        return ExpressionLookBack.EMPTY.andThen(Stream.concat(
                where_clause_.map(c -> c.getLookBack()).map(Stream::of).orElseGet(Stream::empty),
                rules_.stream().map(rule -> rule.getLookBack())
        ));
    }
}
