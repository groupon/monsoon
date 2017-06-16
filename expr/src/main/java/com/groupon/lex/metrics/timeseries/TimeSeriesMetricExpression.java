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

import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.timeseries.expression.Context;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

/**
 *
 * @author ariane
 */
public interface TimeSeriesMetricExpression extends Function<Context<?>, TimeSeriesMetricDeltaSet>, PrintableExpression {
    public static final TimeSeriesMetricExpression TRUE = new TimeSeriesMetricExpression() {
        private final TimeSeriesMetricDeltaSet VALUE = new TimeSeriesMetricDeltaSet(MetricValue.TRUE);

        @Override
        public TimeSeriesMetricDeltaSet apply(Context<?> ctx) {
            return VALUE;
        }

        @Override
        public int getPriority() {
            return BRACKETS;
        }

        @Override
        public StringBuilder configString() {
            return new StringBuilder("true");
        }

        @Override
        public Collection<TimeSeriesMetricExpression> getChildren() { return Collections.EMPTY_LIST; }
    };

    public static final TimeSeriesMetricExpression FALSE = new TimeSeriesMetricExpression() {
        private final TimeSeriesMetricDeltaSet VALUE = new TimeSeriesMetricDeltaSet(MetricValue.FALSE);

        @Override
        public TimeSeriesMetricDeltaSet apply(Context<?> ctx) {
            return VALUE;
        }

        @Override
        public int getPriority() {
            return BRACKETS;
        }

        @Override
        public StringBuilder configString() {
            return new StringBuilder("false");
        }

        @Override
        public Collection<TimeSeriesMetricExpression> getChildren() { return Collections.EMPTY_LIST; }
    };

    @Override
    public TimeSeriesMetricDeltaSet apply(Context<?> ctx);

    public Collection<TimeSeriesMetricExpression> getChildren();

    /**
     * @return A filter that describes how far back the expression may reach in
     *     the collection history.
     */
    public default ExpressionLookBack getLookBack() {
        return ExpressionLookBack.EMPTY.andThen(getChildren().stream().map(TimeSeriesMetricExpression::getLookBack));
    }
}
