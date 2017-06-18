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
import com.groupon.lex.metrics.config.MatchStatement;
import com.groupon.lex.metrics.config.MatchStatement.IdentifierPair;
import com.groupon.lex.metrics.config.MatchStatement.LookBackExposingPredicate;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TimeSeriesTransformer;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.timeseries.expression.MutableContext;
import java.util.Collection;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
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
        public Stream<Consumer<MutableContext>> map(Stream<Consumer<MutableContext>> in, Context ctx);
    }

    /**
     * Create a decorator map from a mapping of GroupMatchers.
     */
    private static DecoratorMap group_map_(Map<String, PathMatcher> paths) {
        if (paths.isEmpty()) return concat();

        return new DecoratorMap() {
            @Override
            public Stream<Consumer<MutableContext>> map(Stream<Consumer<MutableContext>> in, Context ctx) {
                final Map<String, TimeSeriesValueSet> pathMapping = paths.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                pmEntry -> {
                                    return ctx.getTSData()
                                    .getCurrentCollection()
                                    .get(p -> pmEntry.getValue().match(p.getPath()), x -> true);
                                }));

                for (final Map.Entry<String, TimeSeriesValueSet> pm
                             : pathMapping.entrySet()) {
                    final String identifier = pm.getKey();
                    final List<Consumer<MutableContext>> applications = pm.getValue().stream()
                            .map(group -> {
                                Consumer<MutableContext> application = (nestedCtx) -> nestedCtx.putGroupAliasByName(identifier, group::getGroup);
                                return application;
                            })
                            .collect(Collectors.toList());

                    in = in.flatMap(inApplication -> applications.stream().map(inApplication::andThen));
                }
                return in;
            }
        };
    }

    /**
     * Create a decorator map from a mapping of metric matchers.
     */
    private static DecoratorMap metric_map_(Map<IdentifierPair, MetricMatcher> matchers) {
        if (matchers.isEmpty()) return concat();

        return new DecoratorMap() {
            @Override
            public Stream<Consumer<MutableContext>> map(Stream<Consumer<MutableContext>> in, Context ctx) {
                final Map<IdentifierPair, List<Map.Entry<MetricMatcher.MatchedName, MetricValue>>> metricMapping = matchers.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                mmEntry -> mmEntry.getValue().filter(ctx).collect(Collectors.toList())));

                for (final Map.Entry<IdentifierPair, List<Map.Entry<MetricMatcher.MatchedName, MetricValue>>> mm
                     : metricMapping.entrySet()) {
                    final String groupIdentifier = mm.getKey().getGroup();
                    final String metricIdentifier = mm.getKey().getMetric();
                    final List<Consumer<MutableContext>> applications = mm.getValue().stream()
                            .map(matchedMetric -> {
                                Consumer<MutableContext> application = (nestedCtx) -> {
                                    nestedCtx.putGroupAliasByName(groupIdentifier, matchedMetric.getKey().getMatchedGroup()::getGroup);
                                    nestedCtx.putMetricAliasByName(metricIdentifier, matchedMetric.getKey().getMatchedGroup()::getGroup, matchedMetric.getKey()::getMetric);
                                };
                                return application;
                            })
                            .collect(Collectors.toList());

                    in = in.flatMap(inApplication -> applications.stream().map(inApplication::andThen));
                }
                return in;
            }
        };
    }

    /**
     * Create a decorator map, by chaining zero or more decorator maps.
     */
    private static DecoratorMap concat(DecoratorMap... d_maps) {
        return (Stream<Consumer<MutableContext>> in, Context ctx) -> {
            for (DecoratorMap d_map : d_maps) {
                in = d_map.map(in, ctx);
            }
            return in;
        };
    }

    private void play_rules_(Context ctx) {
        rules_.forEach(rule -> rule.transform(ctx));
    }

    @Override
    public void transform(Context parent) {
        decorator_
                .map(Stream.of(
                        (MutableContext mctx) -> {
                            /* SKIP */
                        }),
                        parent)
                .map(application -> {
                    final MutableContext ctx = new MutableContext(parent);
                    application.accept(ctx);
                    return ctx;
                })
                .filter(where_clause_.orElse(CONTEXT_TAUTOLOGY))
                .forEach(this::play_rules_);
    }

    @Override
    public ExpressionLookBack getLookBack() {
        return ExpressionLookBack.EMPTY.andThen(Stream.concat(
                where_clause_.map(c -> c.getLookBack()).map(Stream::of).orElseGet(Stream::empty),
                rules_.stream().map(rule -> rule.getLookBack())
        ));
    }

    private static final LookBackExposingPredicate CONTEXT_TAUTOLOGY = new LookBackExposingPredicate() {
        @Override
        public boolean test(Context t) {
            return true;
        }

        @Override
        public ExpressionLookBack getLookBack() {
            return ExpressionLookBack.EMPTY;
        }
    };
}
