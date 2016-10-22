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
package com.groupon.lex.metrics.expression;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.transformers.LiteralNameResolver;
import com.groupon.lex.metrics.transformers.NameResolver;
import java.util.Optional;
import java.util.stream.Stream;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * @author ariane
 */
@RunWith(MockitoJUnitRunner.class)
public class LiteralGroupExpressionTest {
    @Mock
    private NameResolver resolver;
    @Mock
    private Context ctx;
    @Mock
    private TimeSeriesCollectionPair ts_data;

    private TimeSeriesValue tsv = new MutableTimeSeriesValue(GroupName.valueOf("foobar"));

    @Test
    public void name() {
        final LiteralGroupExpression expr = new LiteralGroupExpression(resolver);

        assertSame(resolver, expr.getName());
    }

    @Test
    public void config_string() {
        final LiteralGroupExpression expr = new LiteralGroupExpression(resolver);

        when(resolver.configString()).thenReturn(new StringBuilder("foobar"));
        assertEquals("foobar", expr.configString().toString());
    }

    @Test
    public void to_string() {
        final LiteralGroupExpression expr = new LiteralGroupExpression(resolver);

        when(resolver.toString()).thenReturn("foobar");
        assertThat(expr.toString(), containsString("foobar"));
    }

    @Test
    public void apply_returning_tsv() {
        final LiteralGroupExpression expr = new LiteralGroupExpression(resolver);
        final TimeSeriesValueSet expect = new TimeSeriesValueSet(Stream.of(tsv));

        when(resolver.apply(ctx)).thenReturn(Optional.of(SimpleGroupPath.valueOf("foobar")));
        when(ctx.getTSData()).thenReturn(ts_data);
        when(ts_data.getTSValue(SimpleGroupPath.valueOf("foobar"))).thenReturn(expect);
        assertEquals(expect, expr.getTSDelta(ctx));
    }

    @Test
    public void apply_returning_no_name() {
        final LiteralGroupExpression expr = new LiteralGroupExpression(resolver);

        when(resolver.apply(ctx)).thenReturn(Optional.empty());
        assertEquals(TimeSeriesValueSet.EMPTY, expr.getTSDelta(ctx));
    }

    @Test
    public void apply_returning_no_tsv() {
        final LiteralGroupExpression expr = new LiteralGroupExpression(resolver);

        when(resolver.apply(ctx)).thenReturn(Optional.of(SimpleGroupPath.valueOf("foobar")));
        when(ctx.getTSData()).thenReturn(ts_data);
        when(ts_data.getTSValue(SimpleGroupPath.valueOf("foobar"))).thenReturn(TimeSeriesValueSet.EMPTY);
        assertEquals(TimeSeriesValueSet.EMPTY, expr.getTSDelta(ctx));
    }

    @Test
    public void equality() {
        final LiteralGroupExpression expr1 = new LiteralGroupExpression(new LiteralNameResolver("foobar"));
        final LiteralGroupExpression expr2 = new LiteralGroupExpression(new LiteralNameResolver("foobar"));

        assertEquals(expr1.hashCode(), expr2.hashCode());
        assertTrue(expr1.equals(expr2));
    }

    @Test
    public void inequality() {
        final LiteralGroupExpression expr1 = new LiteralGroupExpression(new LiteralNameResolver("foobar"));
        final LiteralGroupExpression expr2 = new LiteralGroupExpression(new LiteralNameResolver("other"));

        assertFalse(expr1.equals(null));
        assertFalse(expr1.equals(new Object()));
        assertFalse(expr1.equals(expr2));
    }
}
