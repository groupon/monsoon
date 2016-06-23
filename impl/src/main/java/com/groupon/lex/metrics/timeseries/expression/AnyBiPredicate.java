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
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.lib.TriFunction;
import com.groupon.lex.metrics.timeseries.TagMatchingClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Util.maybeBraces;
import java.util.Optional;

/**
 *
 * @author ariane
 */
public abstract class AnyBiPredicate extends AbstractBiPredicate<MetricValue, MetricValue> {
    private final String comparator_string_;

    public AnyBiPredicate(TimeSeriesMetricExpression x_impl, TimeSeriesMetricExpression y_impl, String comparatorString, int priority, TagMatchingClause matcher) {
        super(x_impl, y_impl, Optional::of, Optional::of, priority, matcher);
        comparator_string_ = comparatorString;
    }

    /* Must implement these. */
    protected abstract Optional<Boolean> predicate(boolean x, boolean y);
    protected abstract Optional<Boolean> predicate(long x, long y);
    protected abstract Optional<Boolean> predicate(double x, double y);
    protected abstract Optional<Boolean> predicate(String x, String y);
    protected abstract Optional<Boolean> predicate(Histogram x, Histogram y);

    /* Defaults, using type coercion. */
    protected Optional<Boolean> predicate(long x, double y) { return predicate((double)x, y); }
    protected Optional<Boolean> predicate(double x, long y) { return predicate(x, (double)y); }
    protected Optional<Boolean> predicate(boolean x, long y) { return predicate((x ? 1 : 0), y); }
    protected Optional<Boolean> predicate(long x, boolean y) { return predicate(x, (y ? 1 : 0)); }
    protected Optional<Boolean> predicate(boolean x, double y) { return predicate((x ? 1D : 0D), y); }
    protected Optional<Boolean> predicate(double x, boolean y) { return predicate(x, (y ? 1D : 0D)); }

    /* Defaults for combinations that don't make sense. */
    protected Optional<Boolean> predicate(boolean x, String y) { return Optional.empty(); }
    protected Optional<Boolean> predicate(long x, String y) { return Optional.empty(); }
    protected Optional<Boolean> predicate(double x, String y) { return Optional.empty(); }
    protected Optional<Boolean> predicate(Histogram x, String y) { return Optional.empty(); }
    protected Optional<Boolean> predicate(String x, boolean y) { return Optional.empty(); }
    protected Optional<Boolean> predicate(String x, long y) { return Optional.empty(); }
    protected Optional<Boolean> predicate(String x, double y) { return Optional.empty(); }
    protected Optional<Boolean> predicate(String x, Histogram y) { return Optional.empty(); }

    /* Histogram combinations, default to empty. */
    protected Optional<Boolean> predicate(boolean x, Histogram y) { return Optional.empty(); }
    protected Optional<Boolean> predicate(long x, Histogram y) { return Optional.empty(); }
    protected Optional<Boolean> predicate(double x, Histogram y) { return Optional.empty(); }
    protected Optional<Boolean> predicate(Histogram x, boolean y) { return Optional.empty(); }
    protected Optional<Boolean> predicate(Histogram x, long y) { return Optional.empty(); }
    protected Optional<Boolean> predicate(Histogram x, double y) { return Optional.empty(); }

    private static Optional<Boolean> unbound_(Object x, Object y, Object z) { return Optional.empty(); }

    private static enum SelType {
        BOOLEAN,
        INT,
        FLOAT,
        STRING,
        HISTOGRAM,
        NONE
    };

    private static SelType sel_type_(MetricValue mv) {
        if (mv.getBoolValue() != null) return SelType.BOOLEAN;
        if (mv.getIntValue() != null) return SelType.INT;
        if (mv.getFltValue() != null) return SelType.FLOAT;
        if (mv.getStrValue() != null) return SelType.STRING;
        if (mv.getHistValue() != null) return SelType.HISTOGRAM;
        return SelType.NONE;
    }

    /**
     * Ugh, really wish I had some template logic here...
     * @param x Selector type for x.
     * @param y Selector type for y.
     * @return A function that will invoke a predicate, based on x and y.
     */
    private static TriFunction<AnyBiPredicate, MetricValue, MetricValue, Optional<Boolean>> select_(SelType x, SelType y) {
        switch (x) {
            case BOOLEAN:
                switch (y) {
                    case BOOLEAN:
                    {
                        TriFunction<AnyBiPredicate, Boolean, Boolean, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getBoolValue).bind3_fn(MetricValue::getBoolValue);
                    }
                    case INT:
                    {
                        TriFunction<AnyBiPredicate, Boolean, Long, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getBoolValue).bind3_fn(MetricValue::getIntValue);
                    }
                    case FLOAT:
                    {
                        TriFunction<AnyBiPredicate, Boolean, Double, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getBoolValue).bind3_fn(MetricValue::getFltValue);
                    }
                    case STRING:
                    {
                        TriFunction<AnyBiPredicate, Boolean, String, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getBoolValue).bind3_fn(MetricValue::getStrValue);
                    }
                    case HISTOGRAM:
                    {
                        TriFunction<AnyBiPredicate, Boolean, Histogram, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getBoolValue).bind3_fn(MetricValue::getHistValue);
                    }
                    case NONE:
                        return AnyBiPredicate::unbound_;
                }
                break;
            case INT:
                switch (y) {
                    case BOOLEAN:
                    {
                        TriFunction<AnyBiPredicate, Long, Boolean, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getIntValue).bind3_fn(MetricValue::getBoolValue);
                    }
                    case INT:
                    {
                        TriFunction<AnyBiPredicate, Long, Long, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getIntValue).bind3_fn(MetricValue::getIntValue);
                    }
                    case FLOAT:
                    {
                        TriFunction<AnyBiPredicate, Long, Double, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getIntValue).bind3_fn(MetricValue::getFltValue);
                    }
                    case STRING:
                    {
                        TriFunction<AnyBiPredicate, Long, String, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getIntValue).bind3_fn(MetricValue::getStrValue);
                    }
                    case HISTOGRAM:
                    {
                        TriFunction<AnyBiPredicate, Long, Histogram, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getIntValue).bind3_fn(MetricValue::getHistValue);
                    }
                    case NONE:
                        return AnyBiPredicate::unbound_;
                }
                break;
            case FLOAT:
                switch (y) {
                    case BOOLEAN:
                    {
                        TriFunction<AnyBiPredicate, Double, Boolean, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getFltValue).bind3_fn(MetricValue::getBoolValue);
                    }
                    case INT:
                    {
                        TriFunction<AnyBiPredicate, Double, Long, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getFltValue).bind3_fn(MetricValue::getIntValue);
                    }
                    case FLOAT:
                    {
                        TriFunction<AnyBiPredicate, Double, Double, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getFltValue).bind3_fn(MetricValue::getFltValue);
                    }
                    case STRING:
                    {
                        TriFunction<AnyBiPredicate, Double, String, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getFltValue).bind3_fn(MetricValue::getStrValue);
                    }
                    case HISTOGRAM:
                    {
                        TriFunction<AnyBiPredicate, Double, Histogram, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getFltValue).bind3_fn(MetricValue::getHistValue);
                    }
                    case NONE:
                        return AnyBiPredicate::unbound_;
                }
                break;
            case STRING:
                switch (y) {
                    case BOOLEAN:
                    {
                        TriFunction<AnyBiPredicate, String, Boolean, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getStrValue).bind3_fn(MetricValue::getBoolValue);
                    }
                    case INT:
                    {
                        TriFunction<AnyBiPredicate, String, Long, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getStrValue).bind3_fn(MetricValue::getIntValue);
                    }
                    case FLOAT:
                    {
                        TriFunction<AnyBiPredicate, String, Double, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getStrValue).bind3_fn(MetricValue::getFltValue);
                    }
                    case STRING:
                    {
                        TriFunction<AnyBiPredicate, String, String, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getStrValue).bind3_fn(MetricValue::getStrValue);
                    }
                    case HISTOGRAM:
                    {
                        TriFunction<AnyBiPredicate, String, Histogram, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getStrValue).bind3_fn(MetricValue::getHistValue);
                    }
                    case NONE:
                        return AnyBiPredicate::unbound_;
                }
                break;
            case HISTOGRAM:
                switch (y) {
                    case BOOLEAN:
                    {
                        TriFunction<AnyBiPredicate, Histogram, Boolean, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getHistValue).bind3_fn(MetricValue::getBoolValue);
                    }
                    case INT:
                    {
                        TriFunction<AnyBiPredicate, Histogram, Long, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getHistValue).bind3_fn(MetricValue::getIntValue);
                    }
                    case FLOAT:
                    {
                        TriFunction<AnyBiPredicate, Histogram, Double, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getHistValue).bind3_fn(MetricValue::getFltValue);
                    }
                    case STRING:
                    {
                        TriFunction<AnyBiPredicate, Histogram, String, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getHistValue).bind3_fn(MetricValue::getStrValue);
                    }
                    case HISTOGRAM:
                    {
                        TriFunction<AnyBiPredicate, Histogram, Histogram, Optional<Boolean>> fn = AnyBiPredicate::predicate;
                        return fn.bind2_fn(MetricValue::getHistValue).bind3_fn(MetricValue::getHistValue);
                    }
                    case NONE:
                        return AnyBiPredicate::unbound_;
                }
                break;
            case NONE:
                switch (y) {
                    case BOOLEAN:
                    case INT:
                    case FLOAT:
                    case STRING:
                    case NONE:
                        return AnyBiPredicate::unbound_;
                }
        }

        throw new IllegalStateException("Unrecognized Selection type");
    }

    @Override
    protected final Optional<Boolean> expr(MetricValue x_val, MetricValue y_val) {
        return select_(sel_type_(x_val), sel_type_(y_val)).apply(this, x_val, y_val);
    }

    @Override
    public StringBuilder configString() {
        return new StringBuilder()
                .append(maybeBraces(getPriority(), getXArg()))
                .append(' ')
                .append(comparator_string_)
                .append(' ')
                .append(Optional.of(getMatcher().configString()).filter(sb -> sb.length() > 0).map(sb -> sb.append(' ')).orElseGet(StringBuilder::new))
                .append(maybeBraces(getPriority() + 1, getYArg()));
    }
}
