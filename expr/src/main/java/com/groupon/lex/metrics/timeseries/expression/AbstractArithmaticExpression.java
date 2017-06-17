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
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.timeseries.TagMatchingClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Util.maybeBraces;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ariane
 */
public abstract class AbstractArithmaticExpression extends AbstractBiExpression<Number, Number, Number> {
    private static final Logger LOG = Logger.getLogger(AbstractArithmaticExpression.class.getName());
    protected abstract double expr(double x, double y);
    protected double expr(double x, long y) { return expr(x, (double)y); }
    protected double expr(long x, double y) { return expr((double)x, y); }
    protected abstract long expr(long x, long y);
    protected abstract Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, Histogram y);
    protected Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, long y) { return hist_expr(x, (double)y); }
    protected abstract Optional<Any2<? extends Number, Histogram>> hist_expr(Histogram x, double y);
    protected Optional<Any2<? extends Number, Histogram>> hist_expr(long x, Histogram y) { return hist_expr((double)x, y); }
    protected abstract Optional<Any2<? extends Number, Histogram>> hist_expr(double x, Histogram y);

    private final int priority_;
    private final String operand_;

    private static Optional<Any2<? extends Number, Histogram>> extract_(MetricValue mv) {
        Optional<Any2<? extends Number, Histogram>> hist = mv.histogram().map(Any2::right);
        if (hist.isPresent()) return hist;
        return mv.value().map(Any2::left);
    }

    public AbstractArithmaticExpression(TimeSeriesMetricExpression x_arg, TimeSeriesMetricExpression y_arg, String operand, int priority, TagMatchingClause matcher) {
        super(x_arg, y_arg,
                AbstractArithmaticExpression::extract_,
                AbstractArithmaticExpression::extract_,
                matcher);
        priority_ = priority;
        operand_ = operand;
    }

    public String getOperand() { return operand_; }
    @Override
    public int getPriority() { return priority_; }

    @Override
    protected final Optional<Any2<? extends Number, Histogram>> expr(Number x, Number y) {
        try {
            final Optional<? extends Number> result;
            if (x instanceof Long && y instanceof Long)
                result = Optional.of(expr(x.longValue(), y.longValue()));
            else if (x instanceof Long && y instanceof Double)
                result = Optional.of(expr(x.longValue(), y.doubleValue()));
            else if (x instanceof Double && y instanceof Long)
                result = Optional.of(expr(x.doubleValue(), y.longValue()));
            else
                result = Optional.of(expr(x.doubleValue(), y.doubleValue()));
            return result.map(Any2::left);
        } catch (ArithmeticException ex) {
            LOG.log(Level.FINE, "error evaluating " + configString(), ex);
            return Optional.empty();
        }
    }

    @Override
    protected final Optional<Any2<? extends Number, Histogram>> expr(Number x, Histogram y) {
        try {
            if (x instanceof Long)
                return hist_expr(x.longValue(), y);
            else
                return hist_expr(x.doubleValue(), y);
        } catch (ArithmeticException ex) {
            LOG.log(Level.FINE, "error evaluating " + configString(), ex);
            return Optional.empty();
        }
    }

    @Override
    protected final Optional<Any2<? extends Number, Histogram>> expr(Histogram x, Number y) {
        try {
            if (y instanceof Long)
                return hist_expr(x, y.longValue());
            else
                return hist_expr(x, y.doubleValue());
        } catch (ArithmeticException ex) {
            LOG.log(Level.FINE, "error evaluating " + configString(), ex);
            return Optional.empty();
        }
    }

    @Override
    protected final Optional<Any2<? extends Number, Histogram>> expr(Histogram x, Histogram y) {
        return hist_expr(x, y);
    }

    @Override
    public StringBuilder configString() {
        return new StringBuilder()
                .append(maybeBraces(getPriority(), getXArg()))
                .append(' ')
                .append(operand_)
                .append(' ')
                .append(Optional.of(getMatcher().configString()).filter(sb -> sb.length() > 0).map(sb -> sb.append(' ')).orElseGet(StringBuilder::new))
                .append(maybeBraces(getPriority() + 1, getYArg()));
    }
}
