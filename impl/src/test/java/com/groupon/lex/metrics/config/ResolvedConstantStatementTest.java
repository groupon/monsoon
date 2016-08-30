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

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.expression.GroupExpression;
import com.groupon.lex.metrics.expression.LiteralGroupExpression;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.transformers.LiteralNameResolver;
import com.groupon.lex.metrics.transformers.NameResolver;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singletonMap;
import java.util.HashMap;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * @author ariane
 */
@RunWith(MockitoJUnitRunner.class)
public class ResolvedConstantStatementTest {
    private final ResolvedConstantStatement stmt_single = new ResolvedConstantStatement(
            new LiteralGroupExpression(new LiteralNameResolver("foo")),
            new LiteralNameResolver("x"), MetricValue.fromIntValue(17));

    private final ResolvedConstantStatement stmt_multi = new ResolvedConstantStatement(
            new LiteralGroupExpression(new LiteralNameResolver("foo")),
            new HashMap<NameResolver, MetricValue>() {{
                put(new LiteralNameResolver("x"), MetricValue.fromIntValue(17));
                put(new LiteralNameResolver("y"), MetricValue.fromIntValue(19));
            }});

    private final ResolvedConstantStatement copy_of_stmt_single = new ResolvedConstantStatement(
            new LiteralGroupExpression(new LiteralNameResolver("foo")),
            singletonMap(new LiteralNameResolver("x"), MetricValue.fromIntValue(17)));
    private final ResolvedConstantStatement copy_of_stmt_multi = new ResolvedConstantStatement(
            new LiteralGroupExpression(new LiteralNameResolver("foo")),
            new HashMap<NameResolver, MetricValue>() {{
                put(new LiteralNameResolver("x"), MetricValue.fromIntValue(17));
                put(new LiteralNameResolver("y"), MetricValue.fromIntValue(19));
            }});
    private final ResolvedConstantStatement copy_of_stmt_single_with_other_group = new ResolvedConstantStatement(
            new LiteralGroupExpression(new LiteralNameResolver("bar")),
            singletonMap(new LiteralNameResolver("x"), MetricValue.fromIntValue(17)));

    @Mock
    private GroupExpression group;
    @Mock
    private TimeSeriesCollectionPair ts_data_pair;
    @Mock
    private TimeSeriesCollection ts_data;
    @Mock
    private Context ctx;

    private final TimeSeriesValue tsv = new MutableTimeSeriesValue(DateTime.now(DateTimeZone.UTC), GroupName.valueOf("foo"), EMPTY_MAP);

    @Test
    public void config_string() {
        assertEquals("constant foo x 17;\n", stmt_single.configString().toString());
        assertEquals("constant foo {\n"
                   + "  x 17;\n"
                   + "  y 19;\n"
                   + "}\n",
                stmt_multi.configString().toString());
    }

    @Test
    public void equality() {
        assertEquals(stmt_single.hashCode(), copy_of_stmt_single.hashCode());
        assertTrue(stmt_single.equals(copy_of_stmt_single));

        assertEquals(stmt_multi.hashCode(), copy_of_stmt_multi.hashCode());
        assertTrue(stmt_multi.equals(copy_of_stmt_multi));
    }

    @Test
    public void inequality() {
        assertFalse(stmt_single.equals(null));
        assertFalse(stmt_single.equals(new Object()));
        assertFalse(stmt_single.equals(copy_of_stmt_single_with_other_group));
        assertFalse(stmt_single.equals(copy_of_stmt_multi));
        assertFalse(stmt_multi.equals(copy_of_stmt_single));
    }

    @Test
    public void apply_when_found() {
        when(group.getTSDelta(ctx)).thenReturn(new TimeSeriesValueSet(Stream.of(tsv)));
        when(ctx.getTSData()).thenReturn(ts_data_pair);
        when(ts_data_pair.getCurrentCollection()).thenReturn(ts_data);
        when(ts_data.addMetrics(tsv.getGroup(), singletonMap(MetricName.valueOf("x"), MetricValue.fromIntValue(17)))).thenReturn(ts_data);

        new ResolvedConstantStatement(group, stmt_single.getMetrics()).get().transform(ctx);

        verify(ts_data, times(1)).addMetrics(tsv.getGroup(), singletonMap(MetricName.valueOf("x"), MetricValue.fromIntValue(17)));
        verifyNoMoreInteractions(ts_data);
    }
}
