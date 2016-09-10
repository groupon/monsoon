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
import com.groupon.lex.metrics.Tags;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class WithoutTagAggregationClauseTest {
    @Test
    public void configString() {
        WithoutTagAggregationClause clause = new WithoutTagAggregationClause(Arrays.asList("foo", "bar", "bar", "bar", "1"));

        assertEquals("without ('1', bar, foo)", clause.configString().toString());
    }

    @Test
    public void isNotAScalar() {
        WithoutTagAggregationClause clause = new WithoutTagAggregationClause(Arrays.asList("foo"));

        assertFalse(clause.isScalar());
    }

    @Test
    public void getTags() {
        WithoutTagAggregationClause clause = new WithoutTagAggregationClause(Arrays.asList("foo", "bar", "1"));

        assertThat(clause.getTags(), containsInAnyOrder("foo", "bar", "1"));
    }

    @Test
    public void filter() {
        final Map<String, MetricValue> OUT_METRICS = unmodifiableMap(new HashMap<String, MetricValue>() {{
            put("keep_me", MetricValue.fromStrValue("keep_me"));
        }});
        final Map<String, MetricValue> IN_METRICS = unmodifiableMap(new HashMap<String, MetricValue>() {{
            put("foo", MetricValue.fromStrValue("foo"));
            put("bar", MetricValue.TRUE);
            put("1", MetricValue.FALSE);
            putAll(OUT_METRICS);
        }});

        WithoutTagAggregationClause clause = new WithoutTagAggregationClause(Arrays.asList("foo", "bar", "1"));
        final Map<Tags, Collection<Map<String, MetricValue>>> filtered =
                clause.apply(Stream.of(IN_METRICS), Tags::valueOf, Function.identity());

        assertThat(filtered, hasEntry(equalTo(Tags.valueOf(OUT_METRICS)), contains(equalTo(IN_METRICS))));
        assertEquals(1, filtered.size());
    }

    public void applyBinOp() {
        final Map<String, MetricValue> OUT_METRICS = unmodifiableMap(new HashMap<String, MetricValue>() {{
            put("keep_me", MetricValue.fromStrValue("keep_me"));
        }});
        final Map<String, MetricValue> IN_LEFT = unmodifiableMap(new HashMap<String, MetricValue>() {{
            put("side", MetricValue.fromStrValue("left"));
            putAll(OUT_METRICS);
        }});
        final Map<String, MetricValue> IN_RIGHT = unmodifiableMap(new HashMap<String, MetricValue>() {{
            put("side", MetricValue.fromStrValue("right"));
            putAll(OUT_METRICS);
        }});

        WithoutTagAggregationClause clause = new WithoutTagAggregationClause(Arrays.asList("side"));
        final Map<Tags, Collection<Map<String, MetricValue>>> application =
                clause.apply(Stream.of(IN_LEFT), Stream.of(IN_RIGHT), Tags::valueOf, Tags::valueOf, Function.identity(), Function.identity());

        assertThat(application, hasEntry(equalTo(Tags.valueOf(OUT_METRICS)), containsInAnyOrder(equalTo(IN_LEFT), equalTo(IN_RIGHT))));
        assertEquals(1, application.size());
    }

    @Test
    public void filterExcludesIncompleteTagset() {
        final Map<String, MetricValue> OUT_METRICS = unmodifiableMap(new HashMap<String, MetricValue>() {{
            put("keep_me", MetricValue.fromStrValue("keep_me"));
        }});
        final Map<String, MetricValue> IN_METRICS = unmodifiableMap(new HashMap<String, MetricValue>() {{
            put("bar", MetricValue.TRUE);
            put("1", MetricValue.FALSE);
            putAll(OUT_METRICS);
        }});

        WithoutTagAggregationClause clause = new WithoutTagAggregationClause(Arrays.asList("foo", "bar", "1"));
        final Map<Tags, Collection<Map<String, MetricValue>>> filtered =
                clause.apply(Stream.of(IN_METRICS), Tags::valueOf, Function.identity());

        assertTrue(filtered.isEmpty());
    }

    @Test
    public void applyBinOpExcludesIncompleteTagset() {
        final Map<String, MetricValue> OUT_METRICS = unmodifiableMap(new HashMap<String, MetricValue>() {{
            put("keep_me", MetricValue.fromStrValue("keep_me"));
        }});
        final Map<String, MetricValue> IN_LEFT = unmodifiableMap(new HashMap<String, MetricValue>() {{
            put("side", MetricValue.fromStrValue("left"));
            put("exclude-me", MetricValue.TRUE);
            putAll(OUT_METRICS);
        }});
        final Map<String, MetricValue> IN_RIGHT = unmodifiableMap(new HashMap<String, MetricValue>() {{
            put("side", MetricValue.fromStrValue("right"));
            putAll(OUT_METRICS);
        }});

        WithoutTagAggregationClause clause = new WithoutTagAggregationClause(Arrays.asList("side", "exclude-me"));
        final Map<Tags, Collection<Map<String, MetricValue>>> application =
                clause.apply(Stream.of(IN_LEFT), Stream.of(IN_RIGHT), Tags::valueOf, Tags::valueOf, Function.identity(), Function.identity());

        assertThat(application, hasEntry(equalTo(Tags.valueOf(OUT_METRICS)), contains(equalTo(IN_LEFT))));
        assertEquals(1, application.size());
    }
}
