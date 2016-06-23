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

import com.groupon.lex.metrics.PathMatcher;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.transformers.IdentifierNameResolver;
import java.util.Arrays;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import java.util.Optional;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class MatchStatementTest {
    private final RuleStatement rs = new AliasStatement("H", new IdentifierNameResolver("G"));
    private final PathMatcher matcher = new PathMatcher(new PathMatcher.WildcardMatch());
    private final TimeSeriesMetricExpression pred = TimeSeriesMetricExpression.FALSE;

    @Test
    public void config_string() {
        MatchStatement ms = new MatchStatement(
                singletonMap("G", matcher),
                EMPTY_MAP,
                Optional.empty(),
                singleton(rs));

        assertThat(ms.configString().toString(),
                startsWith("match " + matcher.configString() + " as G {\n"));
        assertThat(ms.configString().toString(),
                endsWith("}\n"));
    }

    @Test
    public void config_string_empty_body() {
        MatchStatement ms = new MatchStatement(
                singletonMap("G", matcher),
                EMPTY_MAP,
                Optional.empty(),
                EMPTY_LIST);

        assertEquals("match " + matcher.configString() + " as G {}\n",
                ms.configString().toString());
    }

    @Test
    public void config_string_with_predicate() {
        MatchStatement ms = new MatchStatement(
                singletonMap("G", matcher),
                EMPTY_MAP,
                Optional.of(pred),
                singleton(rs));

        assertThat(ms.configString().toString(),
                startsWith("match " + matcher.configString() + " as G where " + pred.configString() + " {\n"));
    }

    @Test
    public void to_string() {
        MatchStatement ms = new MatchStatement(
                singletonMap("G", matcher),
                EMPTY_MAP,
                Optional.of(pred),
                singleton(rs));

        assertThat(ms.toString(), startsWith("MatchStatement{"));
        assertThat(ms.toString(), endsWith("}"));
    }

    @Test
    public void equality() {
        MatchStatement ms1 = new MatchStatement(
                singletonMap("G", matcher),
                EMPTY_MAP,
                Optional.of(pred),
                singleton(rs));
        MatchStatement ms2 = new MatchStatement(
                singletonMap("G", matcher),
                EMPTY_MAP,
                Optional.of(pred),
                singleton(rs));

        assertEquals(ms1.hashCode(), ms2.hashCode());
        assertTrue(ms1.equals(ms2));
    }

    @Test
    public void inequality() {
        MatchStatement ms = new MatchStatement(
                singletonMap("G", matcher),
                EMPTY_MAP,
                Optional.of(pred),
                singleton(rs));
        MatchStatement ms_different_alias = new MatchStatement(
                singletonMap("H", matcher),
                EMPTY_MAP,
                Optional.of(pred),
                singleton(rs));
        MatchStatement ms_different_predicate = new MatchStatement(
                singletonMap("G", matcher),
                EMPTY_MAP,
                Optional.of(TimeSeriesMetricExpression.TRUE),
                singleton(rs));
        MatchStatement ms_no_predicate = new MatchStatement(
                singletonMap("G", matcher),
                EMPTY_MAP,
                Optional.of(TimeSeriesMetricExpression.TRUE),
                singleton(rs));
        MatchStatement ms_different_body = new MatchStatement(
                singletonMap("G", matcher),
                EMPTY_MAP,
                Optional.of(pred),
                Arrays.asList(rs, rs));

        assertFalse(ms.equals(ms_different_alias));
        assertFalse(ms.equals(ms_different_predicate));
        assertFalse(ms.equals(ms_no_predicate));
        assertFalse(ms.equals(ms_different_body));
        assertFalse(ms.equals(new Object()));
        assertFalse(ms.equals(null));
    }
}
