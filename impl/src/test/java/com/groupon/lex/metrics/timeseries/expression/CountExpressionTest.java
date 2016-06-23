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

import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.timeseries.TagAggregationClause;
import java.util.Arrays;
import java.util.Collections;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class CountExpressionTest {
    @Test
    public void no_arg_constructor() {
        final CountExpression expr = new CountExpression(Collections.EMPTY_LIST, TagAggregationClause.DEFAULT);

        assertEquals("count()", expr.configString().toString());
    }

    @Test
    public void arg1_constructor() {
        final CountExpression expr = new CountExpression(Arrays.asList(Any2.right(Util.constantExpression("foobar"))), TagAggregationClause.DEFAULT);

        assertEquals("count(\"foobar\")", expr.configString().toString());
    }

    @Test
    public void arg2_constructor() {
        final CountExpression expr = new CountExpression(Arrays.asList(Any2.right(Util.constantExpression("foobar")), Any2.right(Util.constantExpression(17))), TagAggregationClause.DEFAULT);

        assertEquals("count(\"foobar\", 17)", expr.configString().toString());
    }
}
