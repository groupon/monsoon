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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class SelectHandlerTest {
    private static final TimeSeriesMetricFilter GROUP_FILTER = new TimeSeriesMetricFilter()
            .withGroup(new PathMatcher(new PathMatcher.LiteralNameMatch("foo"), new PathMatcher.LiteralNameMatch("bar")))
            .withGroup(new PathMatcher(new PathMatcher.LiteralNameMatch("with"), new PathMatcher.LiteralNameMatch("\"quote\"")))
            .withGroup(new PathMatcher(new PathMatcher.LiteralNameMatch("wildcard"), new PathMatcher.WildcardMatch()))
            .withGroup(new PathMatcher(new PathMatcher.LiteralNameMatch("double wildcard"), new PathMatcher.DoubleWildcardMatch()))
            .withGroup(new PathMatcher(new PathMatcher.LiteralNameMatch("regex"), new PathMatcher.RegexMatch("a+")))
            .withGroup(new PathMatcher(new PathMatcher.LiteralNameMatch("suspect regex"), new PathMatcher.RegexMatch("^a+$")));

    private static final TimeSeriesMetricFilter FIELD_FILTER = new TimeSeriesMetricFilter()
            .withMetric(new MetricMatcher(new PathMatcher(new PathMatcher.RegexMatch("foo")), new PathMatcher(new PathMatcher.LiteralNameMatch("()"))));

    private static final DateTime BEGIN_TS = new DateTime(0, DateTimeZone.UTC);
    private static final String BEGIN_STR = "1970-01-01T00:00:00.000Z";
    private static final DateTime END_TS = BEGIN_TS.plus(Duration.standardHours(1));
    private static final String END_STR = "1970-01-01T01:00:00.000Z";

    private static final String[] GROUP_FILTER_EXPECT = Stream
            .of(
                    "\"foo.bar\"",
                    "\"with.\\\"quote\\\"\"",
                    "/^wildcard\\..*$/",
                    "/^double wildcard\\..*$/",
                    "/^regex\\..*a+.*$/",
                    "/^suspect regex\\..*$/")
            .map(s -> String.format("SELECT *::field FROM %s WHERE time > '" + BEGIN_STR + "' and time <= '" + END_STR + "' GROUP BY * ORDER BY time ASC", s))
            .toArray(String[]::new);

    private static final String[] FIELD_FILTER_EXPECT = {
        "SELECT \"()\"::field FROM /^.*foo.*$/ WHERE time > '" + BEGIN_STR + "' and time <= '" + END_STR + "' GROUP BY * ORDER BY time ASC"
    };

    @Test
    public void parseGroups() {
        SelectHandler handler = new SelectHandler(GROUP_FILTER);

        final List<String> queryStrings = handler.queriesForInterval(BEGIN_TS, END_TS)
                .collect(Collectors.toList());

        assertThat(queryStrings, Matchers.containsInAnyOrder(GROUP_FILTER_EXPECT));
    }

    @Test
    public void parseMetrics() {
        SelectHandler handler = new SelectHandler(FIELD_FILTER);

        final List<String> queryStrings = handler.queriesForInterval(BEGIN_TS, END_TS)
                .collect(Collectors.toList());

        assertThat(queryStrings, Matchers.containsInAnyOrder(FIELD_FILTER_EXPECT));
    }
}
