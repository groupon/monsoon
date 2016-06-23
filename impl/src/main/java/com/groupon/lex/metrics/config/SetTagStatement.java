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
import com.groupon.lex.metrics.config.impl.SetTag;
import com.groupon.lex.metrics.expression.GroupExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesTransformer;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.stream.Collectors;

/**
 *
 * @author ariane
 */
public class SetTagStatement implements RuleStatement {
    private final GroupExpression group_;
    private final Map<String, TimeSeriesMetricExpression> tags_;

    public SetTagStatement(GroupExpression group, String tag, TimeSeriesMetricExpression expr) {
        this(group, singletonMap(tag, expr));
    }

    public SetTagStatement(GroupExpression group, Map<String, TimeSeriesMetricExpression> tags) {
        group_ = requireNonNull(group);
        tags_ = unmodifiableMap(new HashMap<>(requireNonNull(tags)));
    }

    @Override
    public TimeSeriesTransformer get() {
        return new SetTag(group_, tags_);
    }

    @Override
    public StringBuilder configString() {
        final StringBuilder sb = new StringBuilder()
                .append("tag ")
                .append(group_.configString());

        if (tags_.size() == 1) {
            final Map.Entry<String, TimeSeriesMetricExpression> tag_expr = tags_.entrySet().iterator().next();

            return sb
                    .append(" as ")
                    .append(ConfigSupport.maybeQuoteIdentifier(tag_expr.getKey()))
                    .append(" = ")
                    .append(tag_expr.getValue().configString())
                    .append(";\n");
        } else {
            return sb
                    .append(tags_.entrySet().stream()
                            .sorted(Comparator.comparing(Map.Entry::getKey))
                            .map(tag_expr -> ConfigSupport.maybeQuoteIdentifier(tag_expr.getKey()) + " = " + tag_expr.getValue().configString())
                            .collect(Collectors.joining(", ", " { ", " }")));
        }
    }
}
