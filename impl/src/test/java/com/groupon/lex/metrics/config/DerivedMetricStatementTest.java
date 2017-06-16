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

import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.transformers.NameResolver;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singletonMap;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DerivedMetricStatementTest {
    @Mock
    private NameResolver groupResolver, metricResolver1, metricResolver2;

    private TimeSeriesMetricExpression expr_one_plus_two, expr_one, expr_two;
    private Map<String, TimeSeriesMetricExpression> no_tags, one_tag, two_tags;
    private Map<NameResolver, TimeSeriesMetricExpression> no_metrics, one_metric, two_metrics;

    private DerivedMetricStatement stmt_noTags_noMetrics, stmt_noTags_oneMetric, stmt_noTags_twoMetrics,
            stmt_oneTag_noMetrics, stmt_oneTag_oneMetric, stmt_oneTag_twoMetrics,
            stmt_twoTags_noMetrics, stmt_twoTags_oneMetric, stmt_twoTags_twoMetrics;

    @Before
    public void setup() throws Exception {
        when(groupResolver.configString()).then((invocation) -> new StringBuilder("GROUP"));
        when(metricResolver1.configString()).then((invocation) -> new StringBuilder("METRIC_1"));
        when(metricResolver2.configString()).then((invocation) -> new StringBuilder("METRIC_2"));

        expr_one_plus_two = TimeSeriesMetricExpression.valueOf("1 + 2");
        expr_one = TimeSeriesMetricExpression.valueOf("1");
        expr_two = TimeSeriesMetricExpression.valueOf("2");

        no_tags = EMPTY_MAP;
        one_tag = singletonMap("name", expr_one_plus_two);
        two_tags = new HashMap<String, TimeSeriesMetricExpression>() {
            {
                put("x", expr_one);
                put("y", expr_two);
            }
        };

        no_metrics = EMPTY_MAP;
        one_metric = singletonMap(metricResolver1, expr_one_plus_two);
        two_metrics = new HashMap<NameResolver, TimeSeriesMetricExpression>() {
            {
                put(metricResolver1, expr_one);
                put(metricResolver2, expr_two);
            }
        };

        stmt_noTags_noMetrics = new DerivedMetricStatement(groupResolver, no_tags, no_metrics);
        stmt_noTags_oneMetric = new DerivedMetricStatement(groupResolver, no_tags, one_metric);
        stmt_noTags_twoMetrics = new DerivedMetricStatement(groupResolver, no_tags, two_metrics);
        stmt_oneTag_noMetrics = new DerivedMetricStatement(groupResolver, one_tag, no_metrics);
        stmt_oneTag_oneMetric = new DerivedMetricStatement(groupResolver, one_tag, one_metric);
        stmt_oneTag_twoMetrics = new DerivedMetricStatement(groupResolver, one_tag, two_metrics);
        stmt_twoTags_noMetrics = new DerivedMetricStatement(groupResolver, two_tags, no_metrics);
        stmt_twoTags_oneMetric = new DerivedMetricStatement(groupResolver, two_tags, one_metric);
        stmt_twoTags_twoMetrics = new DerivedMetricStatement(groupResolver, two_tags, two_metrics);
    }

    @Test
    public void configString_noTags_noMetrics() {
        assertEquals("define GROUP {}\n",
                stmt_noTags_noMetrics.configString().toString());

        verify(groupResolver, times(1)).configString();
        verifyNoMoreInteractions(groupResolver);
        verifyZeroInteractions(metricResolver1, metricResolver2);
    }

    @Test
    public void configString_noTags_oneMetric() {
        assertEquals("define GROUP METRIC_1 = " + expr_one_plus_two.configString() + ";\n",
                stmt_noTags_oneMetric.configString().toString());

        verify(groupResolver, times(1)).configString();
        verify(metricResolver1, times(1)).configString();
        verifyNoMoreInteractions(groupResolver, metricResolver1);
        verifyZeroInteractions(metricResolver2);
    }

    @Test
    public void configString_noTags_twoMetrics() {
        assertEquals("define GROUP {\n"
                + "    METRIC_1 = " + expr_one.configString() + ";\n"
                + "    METRIC_2 = " + expr_two.configString() + ";\n"
                + "}\n",
                stmt_noTags_twoMetrics.configString().toString());

        verify(groupResolver, times(1)).configString();
        verify(metricResolver1, times(1)).configString();
        verify(metricResolver2, times(1)).configString();
        verifyNoMoreInteractions(groupResolver, metricResolver1, metricResolver2);
    }

    @Test
    public void configString_oneTag_noMetrics() {
        assertEquals("define GROUP tag(name = " + expr_one_plus_two.configString() + ") {}\n",
                stmt_oneTag_noMetrics.configString().toString());

        verify(groupResolver, times(1)).configString();
        verifyNoMoreInteractions(groupResolver);
        verifyZeroInteractions(metricResolver1, metricResolver2);
    }

    @Test
    public void configString_oneTag_oneMetric() {
        assertEquals("define GROUP tag(name = " + expr_one_plus_two.configString() + ") METRIC_1 = " + expr_one_plus_two.configString() + ";\n",
                stmt_oneTag_oneMetric.configString().toString());

        verify(groupResolver, times(1)).configString();
        verify(metricResolver1, times(1)).configString();
        verifyNoMoreInteractions(groupResolver, metricResolver1);
        verifyZeroInteractions(metricResolver2);
    }

    @Test
    public void configString_oneTag_twoMetrics() {
        assertEquals("define GROUP tag(name = " + expr_one_plus_two.configString() + ") {\n"
                + "    METRIC_1 = " + expr_one.configString() + ";\n"
                + "    METRIC_2 = " + expr_two.configString() + ";\n"
                + "}\n",
                stmt_oneTag_twoMetrics.configString().toString());

        verify(groupResolver, times(1)).configString();
        verify(metricResolver1, times(1)).configString();
        verify(metricResolver2, times(1)).configString();
        verifyNoMoreInteractions(groupResolver, metricResolver1, metricResolver2);
    }

    @Test
    public void configString_twoTags_noMetrics() {
        assertEquals("define GROUP tag(\n"
                + "    x = " + expr_one.configString() + ",\n"
                + "    y = " + expr_two.configString() + ") {}\n",
                stmt_twoTags_noMetrics.configString().toString());

        verify(groupResolver, times(1)).configString();
        verifyNoMoreInteractions(groupResolver);
        verifyZeroInteractions(metricResolver1, metricResolver2);
    }

    @Test
    public void configString_twoTags_oneMetric() {
        assertEquals("define GROUP tag(\n"
                + "    x = " + expr_one.configString() + ",\n"
                + "    y = " + expr_two.configString() + ") METRIC_1 = " + expr_one_plus_two.configString() + ";\n",
                stmt_twoTags_oneMetric.configString().toString());

        verify(groupResolver, times(1)).configString();
        verify(metricResolver1, times(1)).configString();
        verifyNoMoreInteractions(groupResolver, metricResolver1);
        verifyZeroInteractions(metricResolver2);
    }

    @Test
    public void configString_twoTags_twoMetrics() {
        assertEquals("define GROUP tag(\n"
                + "    x = " + expr_one.configString() + ",\n"
                + "    y = " + expr_two.configString() + ") {\n"
                + "    METRIC_1 = " + expr_one.configString() + ";\n"
                + "    METRIC_2 = " + expr_two.configString() + ";\n"
                + "}\n",
                stmt_twoTags_twoMetrics.configString().toString());

        verify(groupResolver, times(1)).configString();
        verify(metricResolver1, times(1)).configString();
        verify(metricResolver2, times(1)).configString();
        verifyNoMoreInteractions(groupResolver, metricResolver1, metricResolver2);
    }

    @Test
    public void toString_is_configString() {
        assertEquals(stmt_noTags_noMetrics.configString().toString(), stmt_noTags_noMetrics.toString());
        assertEquals(stmt_noTags_oneMetric.configString().toString(), stmt_noTags_oneMetric.toString());
        assertEquals(stmt_noTags_twoMetrics.configString().toString(), stmt_noTags_twoMetrics.toString());
        assertEquals(stmt_oneTag_noMetrics.configString().toString(), stmt_oneTag_noMetrics.toString());
        assertEquals(stmt_oneTag_oneMetric.configString().toString(), stmt_oneTag_oneMetric.toString());
        assertEquals(stmt_oneTag_twoMetrics.configString().toString(), stmt_oneTag_twoMetrics.toString());
        assertEquals(stmt_twoTags_noMetrics.configString().toString(), stmt_twoTags_noMetrics.toString());
        assertEquals(stmt_twoTags_oneMetric.configString().toString(), stmt_twoTags_oneMetric.toString());
        assertEquals(stmt_twoTags_twoMetrics.configString().toString(), stmt_twoTags_twoMetrics.toString());
    }
}
