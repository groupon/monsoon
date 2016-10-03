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
package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.ConfigSupport;
import com.groupon.lex.metrics.MetricMatcher;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.config.ConfigStatement;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.timeseries.expression.PreviousContextWrapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public abstract class TimeSeriesMetricAggregate<T> implements TimeSeriesMetricExpression {
    private static final Logger LOG = Logger.getLogger(TimeSeriesMetricAggregate.class.getName());
    private final String fn_name_;
    private final Collection<MetricMatcher> matchers_;
    private final Collection<TimeSeriesMetricExpression> exprs_;
    private final TagAggregationClause aggregation_;
    private final Optional<Duration> tDelta;

    public TimeSeriesMetricAggregate(String fn_name, Collection<Any2<MetricMatcher, TimeSeriesMetricExpression>> matchers, TagAggregationClause aggregation, Optional<Duration> tDelta) {
        fn_name_ = requireNonNull(fn_name);
        matchers_ = new ArrayList<>(matchers.stream()
                .map((elem) -> elem.getLeft())
                .flatMap((x) -> x.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toList()));
        exprs_ = unmodifiableList(new ArrayList<>(matchers.stream()
                .map((elem) -> elem.getRight())
                .flatMap((x) -> x.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toList())));
        aggregation_ = requireNonNull(aggregation);
        this.tDelta = requireNonNull(tDelta);
    }

    @Override
    public Collection<TimeSeriesMetricExpression> getChildren() { return exprs_; }

    @Override
    public final ExpressionLookBack getLookBack() {
        final ChainableExpressionLookBack base = tDelta
                .map(ExpressionLookBack::fromInterval)
                .orElse(ExpressionLookBack.EMPTY);
        return base.andThen(getChildren().stream().map(TimeSeriesMetricExpression::getLookBack));
    }

    protected abstract T initial_();
    protected MetricValue scalar_fallback_() { return MetricValue.EMPTY; }
    protected abstract T map_(MetricValue x);
    protected abstract T reducer_(T x, T y);
    protected abstract MetricValue unmap_(T v);

    /**
     * Create a reduction of the matchers and expressions for a given context.
     * @param ctx The context on which to apply the reduction.
     * @return A reduction.
     */
    private Intermediate mapAndReduce(Context ctx) {
        /* Fetch each metric wildcard and add it to the to-be-processed list. */
        final List<Map.Entry<Tags, MetricValue>> matcher_tsvs = matchers_.stream()
                .flatMap(m -> m.filter(ctx))
                .map(named_entry -> SimpleMapEntry.create(named_entry.getKey().getTags(), named_entry.getValue()))
                .collect(Collectors.toList());
        /* Resolve each expression. */
        final Map<Boolean, List<TimeSeriesMetricDeltaSet>> expr_tsvs_map = exprs_.stream()
                .map(expr -> expr.apply(ctx))
                .collect(Collectors.partitioningBy(TimeSeriesMetricDeltaSet::isVector));
        final List<TimeSeriesMetricDeltaSet> expr_tsvs = expr_tsvs_map.getOrDefault(true, Collections.emptyList());
        final Optional<T> scalar = expr_tsvs_map.getOrDefault(false, Collections.emptyList())
                .stream()
                .map(tsv_set -> map_(tsv_set.asScalar().get()))
                .reduce(this::reducer_);

        /*
         * Reduce everything using the reducer (in the derived class).
         */
        return new Intermediate(scalar, unmodifiableMap(aggregation_.apply(
                        matcher_tsvs.stream(), expr_tsvs.stream().flatMap(TimeSeriesMetricDeltaSet::streamAsMap),
                        Map.Entry::getKey, Map.Entry::getKey,
                        Map.Entry::getValue, Map.Entry::getValue)
                .entrySet().stream()
                .map(entry -> {
                    return entry.getValue().stream().map(this::map_)
                            .reduce(this::reducer_)
                            .map(v -> SimpleMapEntry.create(entry.getKey(), v));
                })
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()))));
    }

    @Override
    public final TimeSeriesMetricDeltaSet apply(Context ctx) {
        /*
         * If a time delta is specified, create a context for each of the time deltas.
         * Reduce everything using the reducer (in the derived class).
         * If a time delta is specified, try to utilize the cache on previous contexts.
         */
        Intermediate map = tDelta
                .map(tdelta_val -> {
                    return ctx.getTSData().getCollectionPairsSince(tdelta_val).parallelStream()
                            .map(tsdata -> new PreviousContextWrapper(ctx, tsdata))
                            .map(c -> mapAndReduce(c))
                            .reduce(new Intermediate(), this::reduceIntermediate_);
                })
                .orElseGet(() -> mapAndReduce(ctx));

        final TimeSeriesMetricDeltaSet result;
        if (aggregation_.isScalar())
            result = new TimeSeriesMetricDeltaSet(map.finalizeAsScalar());
        else
            result = new TimeSeriesMetricDeltaSet(map.finalizeAsVector());
        LOG.log(Level.FINE, "{0} yields {1}", new Object[]{fn_name_, result});
        return result;
    }

    protected String configStringArgs() {
        return Stream.concat(matchers_.stream().map(MetricMatcher::configString), exprs_.stream().map(ConfigStatement::configString))
                .collect(Collectors.joining(", "));
    }

    @Override
    public StringBuilder configString() {
        final StringBuilder rv = new StringBuilder()
                .append(fn_name_)
                .append(tDelta.map(ConfigSupport::durationConfigString).map(s -> "[" + s + "]").orElse(""))
                .append('(')
                .append(configStringArgs())
                .append(')');

        final StringBuilder agg_cfg = aggregation_.configString();
        if (agg_cfg.length() > 0) rv.append(' ').append(agg_cfg);

        return rv;
    }

    @RequiredArgsConstructor
    @Getter
    private class Intermediate {
        @NonNull
        private final Optional<T> scalar;
        @NonNull
        private final Map<Tags, T> vectors;

        public Intermediate() {
            this(Optional.empty(), emptyMap());
        }

        public MetricValue finalizeAsScalar() {
            return Stream.concat(scalar.map(Stream::of).orElseGet(Stream::empty), vectors.values().stream())
                    .reduce(TimeSeriesMetricAggregate.this::reducer_)
                    .map(TimeSeriesMetricAggregate.this::unmap_)
                    .orElseGet(TimeSeriesMetricAggregate.this::scalar_fallback_);
        }

        public Stream<Map.Entry<Tags, MetricValue>> finalizeAsVector() {
            final T init = initial_();
            final T initScalar = scalar.map(x -> reducer_(init, x)).orElse(init);
            return vectors.entrySet().stream()
                    .map(entry -> SimpleMapEntry.create(entry.getKey(), unmap_(reducer_(initScalar, entry.getValue()))));
        }
    }

    private Intermediate reduceIntermediate_(Intermediate x, Intermediate y) {
        final Optional<T> scalar;
        if (x.getScalar().isPresent() && y.getScalar().isPresent())
            scalar = Optional.of(reducer_(x.getScalar().get(), y.getScalar().get()));
        else if (x.getScalar().isPresent())
            scalar = x.getScalar();
        else if (y.getScalar().isPresent())
            scalar = y.getScalar();
        else
            scalar = Optional.empty();

        final Map<Tags, T> vectors = aggregation_.apply(
                    x.getVectors().entrySet().stream(), y.getVectors().entrySet().stream(),
                    Map.Entry::getKey, Map.Entry::getKey,
                    Map.Entry::getValue, Map.Entry::getValue)
                .entrySet().stream()
                .map(entry -> {
                    return entry.getValue().stream()
                            .reduce(this::reducer_)
                            .map(v -> SimpleMapEntry.create(entry.getKey(), v));
                })
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new Intermediate(scalar, vectors);
    }
}
