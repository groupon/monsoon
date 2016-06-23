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
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.timeseries.expression.ContextIdentifier;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class TimeSeriesMetricExpressionTest {
    /**
     * Support class for this test: none of the context methods may be called.
     */
    private static class FailingContext implements Context {
        @Override
        public Consumer<Alert> getAlertManager() {
            fail("Context should not be invoked.");
            return null;
        }

        @Override
        public TimeSeriesCollectionPair getTSData() {
            fail("Context should not be invoked.");
            return null;
        }

        @Override
        public Map<String, ContextIdentifier> getAllIdentifiers() {
            fail("Context should not be invoked.");
            return null;
        }
    }

    private FailingContext ctx;

    @Before
    public void setup() {
        ctx = new FailingContext();
    }

    @Test
    public void true_expression() {
        assertEquals("true", TimeSeriesMetricExpression.TRUE.configString().toString());
        assertEquals(BRACKETS, TimeSeriesMetricExpression.TRUE.getPriority());
        assertEquals(Optional.of(MetricValue.TRUE), TimeSeriesMetricExpression.TRUE.apply(ctx).asScalar());
    }

    @Test
    public void false_expression() {
        assertEquals("false", TimeSeriesMetricExpression.FALSE.configString().toString());
        assertEquals(BRACKETS, TimeSeriesMetricExpression.FALSE.getPriority());
        assertEquals(Optional.of(MetricValue.FALSE), TimeSeriesMetricExpression.FALSE.apply(ctx).asScalar());
    }
}
