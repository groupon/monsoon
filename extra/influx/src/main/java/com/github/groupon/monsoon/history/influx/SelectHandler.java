/*
 * Copyright (c) 2017, ariane
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.groupon.monsoon.history.influx;

import com.groupon.lex.metrics.MetricMatcher;
import com.groupon.lex.metrics.PathMatcher;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricFilter;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import org.joda.time.DateTime;

public class SelectHandler {
    private final List<String> selectStmts;

    public SelectHandler(@NonNull TimeSeriesMetricFilter filter) {
        selectStmts = unmodifiableList(Stream
                .concat(
                        filter.getMetrics().stream().map(SelectHandler::metricToSelectStmt),
                        filter.getGroups().stream().map(SelectHandler::groupToSelectStmt))
                .collect(Collectors.toList()));
    }

    public Stream<String> queriesForInterval(@NonNull DateTime begin, @NonNull DateTime end) {
        final String querySuffix = " WHERE time > " + begin.toString() + " and time <= " + end.toString() + " GROUP BY * ORDER BY time ASC";
        return selectStmts.stream().map(select -> select + querySuffix);
    }

    private static String groupToSelectStmt(PathMatcher group) {
        return "SELECT *::field FROM " + matcherToString(group);
    }

    private static String metricToSelectStmt(MetricMatcher metric) {
        return "SELECT " + matcherToString(metric.getMetric()) + "::field FROM " + matcherToString(metric.getGroups());
    }

    private static String matcherToString(PathMatcher matcher) {
        return Stream.<Supplier<Optional<String>>>builder()
                .add(() -> matcher.asLiteral().map(path -> String.join(".", path.getPath())).map(SelectHandler::quoteName))
                .add(() -> matcher.visitNonLiteral(new RegexVisitor()).map(RegexVisitor::toRegex).map(SelectHandler::quotePattern))
                .build()
                .map(Supplier::get)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst()
                .orElseThrow(RuntimeException::new);
    }

    private static String quoteName(String name) {
        return '"' + name.replace("\"", "\\\"") + '"';
    }

    private static String quotePattern(String pattern) {
        return "/" + pattern.replace("/", "\\/") + "/";
    }

    private static class RegexVisitor implements PathMatcher.Visitor {
        private static final Pattern SUSPECT_PATTERN_CHARACTERS = Pattern.compile(Pattern.quote("^") + "|" + Pattern.quote("$"));
        private final StringBuilder pattern = new StringBuilder();

        @Override
        public void accept(PathMatcher.LiteralNameMatch match) {
            pattern.append(Pattern.quote(match.getLiteral()));
        }

        @Override
        public void accept(PathMatcher.RegexMatch match) {
            if (SUSPECT_PATTERN_CHARACTERS.matcher(match.getRegex()).find())
                pattern.append(".*");
            else
                pattern.append(match.getRegex());
        }

        @Override
        public void accept(PathMatcher.WildcardMatch match) {
            pattern.append(".*");
        }

        @Override
        public void accept(PathMatcher.DoubleWildcardMatch match) {
            pattern.append(".*");
        }

        public String toRegex() {
            return "^" + pattern.toString() + "$";
        }
    }
}
