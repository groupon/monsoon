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
import com.groupon.lex.metrics.Path;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricFilter;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import com.groupon.lex.metrics.transformers.NameResolver;
import java.util.Collection;
import static java.util.Collections.EMPTY_LIST;
import java.util.List;
import lombok.NonNull;
import lombok.Value;

/**
 * Invoke a name resolver and convert its result to a metric value.
 *
 * If the Name Resolver yields:
 * - [ "true" ] --&gt; yield a boolean true value
 * - [ "false" ] --&gt; yield a boolean false value
 * - [ integral-number] --&gt; yield a metric value representing the number
 * - [ FP-number ] --&gt; yield a metric value representing the number
 * - anything else --&gt; yield a String metric value, separating components by dots ('.').
 */
@Value
public class NameExpression implements TimeSeriesMetricExpression {
    @NonNull
    private final NameResolver resolver;

    private static MetricValue strAsMetricValue(String str) {
        // Try to coerce elem to a boolean.
        if ("true".equals(str)) return MetricValue.TRUE;
        if ("false".equals(str)) return MetricValue.FALSE;
        // Try to coerce elem to an integral number.
        try {
            return MetricValue.fromIntValue(Long.parseLong(str));
        } catch (NumberFormatException e) {
            // Ignore.
        }
        // Try to coerce elem to a floating point number.
        try {
            return MetricValue.fromDblValue(Double.parseDouble(str));
        } catch (NumberFormatException e) {
            // Ignore.
        }
        // Give up and just yield a string.
        return MetricValue.fromStrValue(str);
    }

    private static MetricValue pathAsMetricValue(Path path) {
        final List<String> pathElems = path.getPath();
        if (pathElems.size() == 1) return strAsMetricValue(pathElems.get(0));
        return MetricValue.fromStrValue(String.join(".", pathElems));
    }

    @Override
    public TimeSeriesMetricDeltaSet apply(Context ctx) {
        return resolver.apply(ctx)
                .map(NameExpression::pathAsMetricValue)
                .map(TimeSeriesMetricDeltaSet::new)
                .orElseGet(TimeSeriesMetricDeltaSet::new);
    }

    @Override
    public Collection<TimeSeriesMetricExpression> getChildren() {
        return EMPTY_LIST;
    }

    @Override
    public TimeSeriesMetricFilter getNameFilter() {
        /*
         * Return an empty filter:
         * - in the case identifiers are referenced, the identifier match will populate the filter.
         * - in the case of a literal name, the group/metric is not required to do the transformation.
         */
        return new TimeSeriesMetricFilter();
    }

    @Override
    public int getPriority() {
        return BRACKETS;
    }

    @Override
    public StringBuilder configString() {
        return new StringBuilder()
                .append("name(")
                .append(resolver.configString())
                .append(')');
    }
}
