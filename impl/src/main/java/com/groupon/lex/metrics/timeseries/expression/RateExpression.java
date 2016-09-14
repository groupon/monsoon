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

import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.ConfigSupport;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TagMatchingClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import static com.groupon.lex.metrics.timeseries.expression.Util.pairwiseFlatMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class RateExpression implements TimeSeriesMetricExpression {
    private final TimeSeriesMetricExpression arg_;
    private final Optional<Duration> interval_;

    public RateExpression(TimeSeriesMetricExpression arg) {
        this(arg, Optional.empty());
    }

    public RateExpression(TimeSeriesMetricExpression arg, Duration interval) {
        this(arg, Optional.of(interval));
    }

    public RateExpression(TimeSeriesMetricExpression arg, Optional<Duration> interval) {
        arg_ = arg;
        interval_ = interval;
    }

    @Override
    public Collection<TimeSeriesMetricExpression> getChildren() { return Collections.singleton(arg_); }

    private Optional<Double> diff_(double cur, double prev, double interval) {
        return Optional.of((cur - prev) / interval);
    }

    private Optional<Histogram> diff_(Histogram cur, Histogram prev, double interval) {
        try {
            return Optional.of(Histogram.divide(Histogram.subtract(cur, prev), interval));
        } catch (IllegalArgumentException ex) {
            return Optional.empty();
        }
    }

    private Optional<Any2<Double, Histogram>> apply_(Any2<Double, Histogram> cur, Any2<Double, Histogram> prev, Duration interval) {
        return Optional.of(interval.getMillis())
                .filter(x -> x > 0)
                .map(x -> x / 1000d)  // Interval in seconds.
                .flatMap(x -> {
                    return cur.mapCombine(
                            cur_num -> prev.mapCombine(
                                    prev_num -> diff_(cur_num, prev_num, x).map(Any2::<Double, Histogram>left),
                                    prev_hist -> Optional.<Any2<Double, Histogram>>empty()
                            ),
                            cur_hist -> prev.mapCombine(
                                    prev_num -> Optional.<Any2<Double, Histogram>>empty(),
                                    prev_hist -> diff_(cur_hist, prev_hist, x).map(Any2::<Double, Histogram>right)
                            )
                    );
                });
    }

    private static Optional<Any2<Double, Histogram>> extract_(MetricValue mv) {
        final Optional<Histogram> hist = mv.histogram();
        if (hist.isPresent()) return hist.map(Any2::right);
        return mv.value().map(Number::doubleValue).map(Any2::left);
    }

    private Optional<MetricValue> apply_(MetricValue cur, MetricValue prev, Duration interval) {
        return pairwiseFlatMap(
                        extract_(cur),
                        extract_(prev),
                        (Any2<Double, Histogram> c, Any2<Double, Histogram> p) -> apply_(c, p, interval))
                .map(result -> result.mapCombine(MetricValue::fromDblValue, MetricValue::fromHistValue));
    }

    @Override
    public TimeSeriesMetricDeltaSet apply(Context ctx) {
        final Context previous = new PreviousContextWrapper(ctx, interval_
                .map(intv -> ctx.getTSData().getPreviousCollectionPairAt(intv))
                .orElseGet(() -> ctx.getTSData().getPreviousCollectionPair(1)));
        final Duration collection_interval = new Duration(
                previous.getTSData().getCurrentCollection().getTimestamp(),
                ctx.getTSData().getCurrentCollection().getTimestamp());

        return TagMatchingClause.DEFAULT.applyOptional(
                arg_.apply(ctx),
                arg_.apply(previous),
                (cur, prev) -> apply_(cur, prev, collection_interval));
    }

    @Override
    public ExpressionLookBack getLookBack() {
        return interval_
                .map(ExpressionLookBack::fromInterval)
                .orElseGet(() -> ExpressionLookBack.fromScrapeCount(1))
                .andThen(arg_.getLookBack());
    }

    @Override
    public int getPriority() {
        return BRACKETS;
    }

    @Override
    public StringBuilder configString() {
        return new StringBuilder()
                .append("rate")
                .append(interval_.map(ConfigSupport::durationConfigString).map(s -> "[" + s + "]").orElse(""))
                .append('(')
                .append(arg_.configString())
                .append(')');
    }
}
