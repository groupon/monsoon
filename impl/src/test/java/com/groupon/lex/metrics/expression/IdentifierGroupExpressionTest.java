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
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import com.groupon.lex.metrics.timeseries.expression.Context;
import java.util.Optional;
import java.util.stream.Stream;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
public class IdentifierGroupExpressionTest {
    private static final String VARNAME = "varname";
    @Mock
    private Context ctx;

    private TimeSeriesValue tsv = new MutableTimeSeriesValue(GroupName.valueOf("foobar"));

    @Test
    public void config_string() {
        final IdentifierGroupExpression expr = new IdentifierGroupExpression(VARNAME);

        assertEquals(VARNAME, expr.configString().toString());
    }

    @Test
    public void to_string() {
        final IdentifierGroupExpression expr = new IdentifierGroupExpression(VARNAME);

        assertThat(expr.toString(), containsString(VARNAME));
    }

    @Test
    public void apply_returning_tsv() {
        final IdentifierGroupExpression expr = new IdentifierGroupExpression(VARNAME);
        final TimeSeriesValueSet expect = new TimeSeriesValueSet(Stream.of(tsv));

        when(ctx.getGroupFromIdentifier(VARNAME)).thenReturn(Optional.of(expect));
        assertEquals(expect, expr.getTSDelta(ctx));
    }

    @Test
    public void apply_returning_no_tsv() {
        final IdentifierGroupExpression expr = new IdentifierGroupExpression(VARNAME);

        when(ctx.getGroupFromIdentifier(VARNAME)).thenReturn(Optional.empty());
        assertEquals(TimeSeriesValueSet.EMPTY, expr.getTSDelta(ctx));
    }

    @Test
    public void equality() {
        final IdentifierGroupExpression expr1 = new IdentifierGroupExpression("foobar");
        final IdentifierGroupExpression expr2 = new IdentifierGroupExpression("foobar");

        assertEquals(expr1.hashCode(), expr2.hashCode());
        assertTrue(expr1.equals(expr2));
    }

    @Test
    public void inequality() {
        final IdentifierGroupExpression expr1 = new IdentifierGroupExpression("foobar");
        final IdentifierGroupExpression expr2 = new IdentifierGroupExpression("other");

        assertFalse(expr1.equals(null));
        assertFalse(expr1.equals(new Object()));
        assertFalse(expr1.equals(expr2));
    }
}
