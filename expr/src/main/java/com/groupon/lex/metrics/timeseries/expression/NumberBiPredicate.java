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

import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.timeseries.TagMatchingClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.lex.metrics.timeseries.expression.Util.maybeBraces;
import static com.groupon.lex.metrics.timeseries.expression.Util.pairwiseFlatMap;
import java.util.Optional;
import java.util.function.BiPredicate;

/**
 *
 * @author ariane
 */
public class NumberBiPredicate extends AbstractBiPredicate<MetricValue, MetricValue> {
    private final BiPredicate<Double, Double> predicate_;
    private final String comparator_string_;

    public NumberBiPredicate(TimeSeriesMetricExpression x_impl, TimeSeriesMetricExpression y_impl, BiPredicate<Double, Double> predicate, String comparatorString, int priority, TagMatchingClause matcher) {
        super(x_impl, y_impl, Optional::of, Optional::of, priority, matcher);
        predicate_ = predicate;
        comparator_string_ = comparatorString;
    }

    @Override
    protected Optional<Boolean> expr(MetricValue x_val, MetricValue y_val) {
        return pairwiseFlatMap(x_val.value(), y_val.value(), (x, y) -> Optional.of(predicate_.test(x.doubleValue(), y.doubleValue())));
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
