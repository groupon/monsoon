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
package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.TagMatchingClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPairInstance;
import static com.groupon.lex.metrics.timeseries.expression.Checks.testResult;
import static com.groupon.lex.metrics.timeseries.expression.Util.constantExpression;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class StringExprTest {
    private Context<?> ctx;

    @Before
    public void setup() {
        ctx = new SimpleContext<>(new TimeSeriesCollectionPairInstance(DateTime.now(DateTimeZone.UTC)), (Alert alert) -> {});
    }

    @Test
    public void identityExpression() {
        testResult("foo", UtilX.identityExpression().apply(constantExpression("foo")).apply(ctx));
    }

    @Test
    public void numberEqualPredicate() {
        testResult(true, UtilX.equalPredicate().apply(constantExpression("foo"), constantExpression("foo"), TagMatchingClause.DEFAULT).apply(ctx));
        testResult(false, UtilX.equalPredicate().apply(constantExpression("bar"), constantExpression("foo"), TagMatchingClause.DEFAULT).apply(ctx));
    }

    @Test
    public void numberNotEqualPredicate() {
        testResult(false, UtilX.notEqualPredicate().apply(constantExpression("foo"), constantExpression("foo"), TagMatchingClause.DEFAULT).apply(ctx));
        testResult(true, UtilX.notEqualPredicate().apply(constantExpression("bar"), constantExpression("foo"), TagMatchingClause.DEFAULT).apply(ctx));
    }

    @Test
    public void regexMatch() {
        testResult(true, UtilX.regexMatch().apply(constantExpression("abba"), "^ab").apply(ctx));
        testResult(false, UtilX.regexMatch().apply(constantExpression("abba"), "^ba").apply(ctx));
    }

    @Test
    public void regexMismatch() {
        testResult(false, UtilX.regexMismatch().apply(constantExpression("abba"), "^ab").apply(ctx));
        testResult(true, UtilX.regexMismatch().apply(constantExpression("abba"), "^ba").apply(ctx));
    }
}
