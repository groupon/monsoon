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
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.expression.Context;
import java.util.Optional;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ParserSupportTest {
    @Mock
    private Context ctx;

    @Test
    public void group_name() throws Exception {
        assertEquals(GroupName.valueOf("com", "groupon", "lex", "\u10ff", "\013"),
                new ParserSupport("com.groupon.'lex'.'\\u10ff'.'\\v'").group_name());
    }

    @Test(expected = ConfigurationException.class)
    public void bad_group_name() throws Exception {
        new ParserSupport("com.17.bla").group_name();
    }

    @Test
    public void expression() throws Exception {
        final TimeSeriesMetricExpression expr = new ParserSupport("17+19").expression();

        assertEquals("17 + 19", expr.configString().toString());

        // Use calculation to check if it actually works properly.
        assertEquals(Optional.of(MetricValue.fromIntValue(17 + 19)), expr.apply(ctx).asScalar());

        verifyZeroInteractions(ctx);
    }

    @Test(expected = ConfigurationException.class)
    public void bad_expression() throws Exception {
        new ParserSupport("17 19").expression();
    }

    @Test
    public void histogram() throws Exception {
        assertEquals(new Histogram(new Histogram.RangeWithCount(0, 1, 12)),
                new ParserSupport("[0..1=12]").histogram());
    }

    @Test(expected = ConfigurationException.class)
    public void bad_histogram() throws Exception {
        new ParserSupport("[0..1..12]").histogram();
    }
}
