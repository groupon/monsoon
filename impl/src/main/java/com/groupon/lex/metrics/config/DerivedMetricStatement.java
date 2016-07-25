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
package com.groupon.lex.metrics.config;

import com.groupon.lex.metrics.ConfigSupport;
import com.groupon.lex.metrics.config.impl.DerivedMetricTransformerImpl;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesTransformer;
import com.groupon.lex.metrics.transformers.NameResolver;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 * @author ariane
 */
public class DerivedMetricStatement implements RuleStatement {
    private final static String INDENT = "    ";
    private final DerivedMetricTransformerImpl impl_;

    public DerivedMetricStatement(NameResolver group, NameResolver metric, TimeSeriesMetricExpression value_expr) {
        impl_ = new DerivedMetricTransformerImpl(group, metric, value_expr);
    }

    public DerivedMetricStatement(NameResolver group, Map<String, TimeSeriesMetricExpression> tag_value_exprs, Map<NameResolver, TimeSeriesMetricExpression> metric_value_exprs) {
        impl_ = new DerivedMetricTransformerImpl(group, tag_value_exprs, metric_value_exprs);
    }

    @Override
    public TimeSeriesTransformer get() {
        return impl_;
    }

    @Override
    public StringBuilder configString() {
        final StringBuilder sb = new StringBuilder()
                .append("define ")
                .append(impl_.getGroup().configString())
                .append(' ');

        if (!impl_.getTags().isEmpty()) {
            sb.append(impl_.getTags().entrySet().stream()
                    .map(tag_mapping -> {
                        final String tagIdent = ConfigSupport.maybeQuoteIdentifier(tag_mapping.getKey()).toString();
                        final String tagExpr = tag_mapping.getValue().configString().toString();
                        return INDENT + tagIdent + " = " + tagExpr;
                    })
                    .collect(Collectors.joining(",\n", "tag (\n", "\n) ")));
        }

        if (impl_.getMapping().size() == 1) {
            Map.Entry<NameResolver, TimeSeriesMetricExpression> entry = impl_.getMapping().entrySet().stream().findAny().get();
            sb
                    .append(entry.getKey().configString())
                    .append(" = ")
                    .append(entry.getValue().configString())
                    .append(";\n");
        } else if (impl_.getMapping().isEmpty()) {
            sb.append("{}\n");
        } else {
            sb.append(impl_.getMapping().entrySet().stream()
                    .sorted(Comparator.comparing(entry -> entry.getKey().configString().toString()))
                    .map(entry -> {
                        final String metricName = entry.getKey().configString().toString();
                        final String metricExpr = entry.getValue().configString().toString();
                        return INDENT + metricName + " = " + metricExpr + ";";
                    })
                    .collect(Collectors.joining("\n", "{\n", "\n}")));
        }
        return sb;
    }

    @Override
    public String toString() {
        return configString().toString();
    }
}
