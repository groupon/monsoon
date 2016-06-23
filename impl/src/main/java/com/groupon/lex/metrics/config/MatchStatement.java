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
import com.groupon.lex.metrics.MetricMatcher;
import com.groupon.lex.metrics.PathMatcher;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.config.impl.MatchTransformerImpl;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.expression.Context;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;

/**
 *
 * @author ariane
 */
public class MatchStatement implements RuleStatement {
    private static final String NEWLINE_INDENT = "\n  ";
    private final Map<String, PathMatcher> group_matchers_ = new TreeMap<>();
    private final Map<IdentifierPair, MetricMatcher> metric_matchers_ = new TreeMap<>();
    private final Optional<TimeSeriesMetricExpression> where_clause_;
    private final List<RuleStatement> rules_ = new ArrayList<>();

    public static interface LookBackExposingPredicate extends Predicate<Context> {
        public ExpressionLookBack getLookBack();
    }

    @Value
    public static final class IdentifierPair implements Comparable<IdentifierPair> {
        private final String group, metric;

        @Override
        public int compareTo(IdentifierPair o) {
            return Comparator
                    .comparing(IdentifierPair::getGroup)
                    .thenComparing(IdentifierPair::getMetric)
                    .compare(this, o);
        }
    }

    public MatchStatement(Map<String, PathMatcher> group_matchers,
            Map<IdentifierPair, MetricMatcher> metric_matchers,
            Optional<TimeSeriesMetricExpression> where_clause,
            Collection<RuleStatement> rules) {
        Stream.concat(group_matchers.keySet().stream(), metric_matchers.keySet().stream().flatMap(pair -> Stream.of(pair.getGroup(), pair.getMetric())))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting())).entrySet().stream()
                .filter(entry -> entry.getValue() != 1L)
                .map(entry -> entry.getKey())
                .forEach(identifier -> {
                    throw new IllegalArgumentException("duplicate identifier: " + identifier);
                });

        group_matchers_.putAll(group_matchers);
        metric_matchers_.putAll(metric_matchers);
        where_clause_ = where_clause;
        rules_.addAll(rules);
    }

    private static LookBackExposingPredicate as_predicate_(TimeSeriesMetricExpression expr) {
        return new LookBackExposingPredicate() {
            @Override
            public ExpressionLookBack getLookBack() {
                return expr.getLookBack();
            }

            @Override
            public boolean test(Context ctx) {
                return expr.apply(ctx)
                        .streamValues()
                        .map(MetricValue::asBool)
                        .flatMap((Optional<Boolean> m) -> m.map(Stream::of).orElseGet(Stream::empty))
                        .anyMatch((Boolean bool) -> bool != false);
            }
        };
    }

    @Override
    public MatchTransformerImpl get() {
        return new MatchTransformerImpl(group_matchers_, metric_matchers_,
                where_clause_.map(MatchStatement::as_predicate_),
                rules_.stream().map(RuleStatement::get));
    }

    @Override
    public StringBuilder configString() {
        final StringBuilder collected_matchers =
                Stream.concat(
                        group_matchers_.entrySet().stream()
                                .map((matcher) -> matcher.getValue().configString()
                                        .append(" as ")
                                        .append(ConfigSupport.maybeQuoteIdentifier(matcher.getKey()))),
                        metric_matchers_.entrySet().stream()
                                .map((matcher) -> matcher.getValue().configString()
                                        .append(" as ")
                                        .append(ConfigSupport.maybeQuoteIdentifier(matcher.getKey().getGroup()))
                                        .append(", ")
                                        .append(ConfigSupport.maybeQuoteIdentifier(matcher.getKey().getMetric())))
                )
                .reduce(new StringBuilder(),
                        (StringBuilder x, StringBuilder y) -> (x.length() == 0 ? y : x.append(", ").append(y)),
                        (StringBuilder x, StringBuilder y) -> (x.length() == 0 ? y : x.append(", ").append(y)));
        final StringBuilder nested = rules_.stream()
                .map(ConfigStatement::configString)
                .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append);
        final String nested_indented = Pattern.compile("\n", Pattern.LITERAL).matcher(nested).replaceAll(Matcher.quoteReplacement(NEWLINE_INDENT));
        final String nested_indented_trim_end = Pattern.compile(" +$").matcher(nested_indented).replaceAll(Matcher.quoteReplacement(""));

        StringBuilder result = new StringBuilder()
                .append("match ")
                .append(collected_matchers);
        where_clause_.ifPresent((TimeSeriesMetricExpression expr) -> {
            result.append(' ')
                    .append("where ")
                    .append(expr.configString());
        });
        if (nested_indented_trim_end.isEmpty())
            result.append(" {}\n");
        else
            result.append(" {")
                    .append(NEWLINE_INDENT)
                    .append(nested_indented_trim_end)
                    .append("}\n");

        return result;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + Objects.hashCode(this.group_matchers_);
        hash = 37 * hash + Objects.hashCode(this.metric_matchers_);
        hash = 37 * hash + Objects.hashCode(this.where_clause_);
        hash = 37 * hash + Objects.hashCode(this.rules_);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MatchStatement other = (MatchStatement) obj;
        if (!Objects.equals(this.group_matchers_, other.group_matchers_)) {
            return false;
        }
        if (!Objects.equals(this.metric_matchers_, other.metric_matchers_)) {
            return false;
        }
        if (!Objects.equals(this.where_clause_, other.where_clause_)) {
            return false;
        }
        if (!Objects.equals(this.rules_, other.rules_)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "MatchStatement{" + "group_matchers_=" + group_matchers_ + ", metric_matchers_=" + metric_matchers_ + ", where_clause_=" + where_clause_ + ", rules_=" + rules_ + '}';
    }
}
