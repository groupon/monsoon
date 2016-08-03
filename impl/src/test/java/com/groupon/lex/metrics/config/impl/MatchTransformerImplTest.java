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
package com.groupon.lex.metrics.config.impl;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricMatcher;
import com.groupon.lex.metrics.PathMatcher;
import com.groupon.lex.metrics.PathMatcher.WildcardMatch;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.config.MatchStatement;
import com.groupon.lex.metrics.expression.IdentifierGroupExpression;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TagMatchingClause;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPairInstance;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesTransformer;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.timeseries.expression.IdentifierMetricSelector;
import com.groupon.lex.metrics.timeseries.expression.MetricSelector;
import com.groupon.lex.metrics.timeseries.expression.SimpleContext;
import com.groupon.lex.metrics.timeseries.expression.TagValueExpression;
import com.groupon.lex.metrics.timeseries.expression.Util;
import com.groupon.lex.metrics.timeseries.expression.UtilX;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singletonMap;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 *
 * @author ariane
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MutableTimeSeriesValue.class)
public class MatchTransformerImplTest {
    private static final Logger LOG = Logger.getLogger(MatchTransformerImplTest.class.getName());
    private final static String IDENT = "x";
    private final static String METRIC_IDENT = "y";
    private final static MetricName METRIC = MetricName.valueOf("a");
    private final static GroupName group1_name = GroupName.valueOf(SimpleGroupPath.valueOf("foo"), singletonMap("id", MetricValue.fromStrValue("group1")));
    private final static GroupName group2_name = GroupName.valueOf(SimpleGroupPath.valueOf("bar"), singletonMap("id", MetricValue.fromStrValue("group1")));
    private final static GroupName group3_name = GroupName.valueOf(SimpleGroupPath.valueOf("foo"), singletonMap("id", MetricValue.fromStrValue("group3")));
    private final static PathMatcher wildcard_matcher = new PathMatcher(new WildcardMatch());
    private final static TimeSeriesMetricExpression is_tag_group1 = UtilX.equalPredicate().apply(
            new TagValueExpression(new IdentifierGroupExpression(IDENT), "id"),
            Util.constantExpression("group1"),
            TagMatchingClause.DEFAULT);

    private static interface InvocationCountingTSTransformer extends TimeSeriesTransformer {
        public int getInvocations();
    }

    /** Invokes IDENT METRIC on the context. */
    private InvocationCountingTSTransformer group_rule;

    /** Invokes METRIC on the context. */
    private InvocationCountingTSTransformer metric_ident_rule;

    private MutableTimeSeriesValue group1;
    private MutableTimeSeriesValue group2;
    private MutableTimeSeriesValue group3;
    private Context ctx;

    @Before
    public void setup() {
        group1 = PowerMockito.mock(MutableTimeSeriesValue.class);
        group2 = PowerMockito.mock(MutableTimeSeriesValue.class);
        group3 = PowerMockito.mock(MutableTimeSeriesValue.class);

        when(group1.getGroup()).thenReturn(group1_name);
        when(group2.getGroup()).thenReturn(group2_name);
        when(group3.getGroup()).thenReturn(group3_name);

        when(group1.getTags()).thenReturn(group1_name.getTags());
        when(group2.getTags()).thenReturn(group2_name.getTags());
        when(group3.getTags()).thenReturn(group3_name.getTags());

        when(group1.findMetric(METRIC)).thenReturn(Optional.of(MetricValue.EMPTY));
        when(group2.findMetric(METRIC)).thenReturn(Optional.of(MetricValue.EMPTY));
        when(group3.findMetric(METRIC)).thenReturn(Optional.of(MetricValue.EMPTY));

        when(group1.getMetrics()).thenReturn(singletonMap(METRIC, MetricValue.EMPTY));
        when(group2.getMetrics()).thenReturn(singletonMap(METRIC, MetricValue.EMPTY));
        when(group3.getMetrics()).thenReturn(singletonMap(METRIC, MetricValue.EMPTY));

        final TimeSeriesCollectionPairInstance ts_data = new TimeSeriesCollectionPairInstance();
        ((MutableTimeSeriesCollection)ts_data.getCurrentCollection()).add(group1);
        ((MutableTimeSeriesCollection)ts_data.getCurrentCollection()).add(group2);
        ((MutableTimeSeriesCollection)ts_data.getCurrentCollection()).add(group3);
        ctx = new SimpleContext(ts_data, (alert) -> {});

        metric_ident_rule = new InvocationCountingTSTransformer() {
            private final TimeSeriesMetricExpression expr = new IdentifierMetricSelector(METRIC_IDENT);
            private int invocations_ = 0;

            @Override
            public void transform(Context ctx) {
                ++invocations_;
                LOG.log(Level.INFO, "Identifier resolves to {0}, {1}", new Object[]{ctx.getGroupFromIdentifier(IDENT), ctx.getMetricFromIdentifier(METRIC_IDENT)});
                expr.apply(ctx);
            }
            @Override
            public int getInvocations() { return invocations_; }
            @Override
            public ExpressionLookBack getLookBack() { return ExpressionLookBack.EMPTY; }
        };

        group_rule = new InvocationCountingTSTransformer() {
            private final TimeSeriesMetricExpression expr = new MetricSelector(new IdentifierGroupExpression(IDENT), METRIC);
            private int invocations_ = 0;

            @Override
            public void transform(Context ctx) {
                ++invocations_;
                LOG.log(Level.INFO, "Identifier resolves to {0}", ctx.getGroupFromIdentifier(IDENT));
                expr.apply(ctx);
            }
            @Override
            public int getInvocations() { return invocations_; }
            @Override
            public ExpressionLookBack getLookBack() { return ExpressionLookBack.EMPTY; }
        };
    }

    @Test
    public void match_statement_once_each() {
        final MatchTransformerImpl matchStmt = new MatchTransformerImpl(
                singletonMap(IDENT, wildcard_matcher),
                EMPTY_MAP,
                Optional.empty(),
                Stream.of(group_rule));

        matchStmt.transform(ctx);

        Mockito.verify(group1, Mockito.atLeastOnce()).getGroup();
        Mockito.verify(group2, Mockito.atLeastOnce()).getGroup();
        Mockito.verify(group3, Mockito.atLeastOnce()).getGroup();
        Mockito.verify(group1, Mockito.times(1)).findMetric(METRIC);
        Mockito.verify(group2, Mockito.times(1)).findMetric(METRIC);
        Mockito.verify(group3, Mockito.times(1)).findMetric(METRIC);
        assertEquals(3, group_rule.getInvocations());
    }

    @Test
    public void match_statement_with_where_clause() {
        final MatchTransformerImpl matchStmt = new MatchTransformerImpl(
                singletonMap(IDENT, wildcard_matcher),
                EMPTY_MAP,
                Optional.of(new MatchStatement.LookBackExposingPredicate() {
                    @Override
                    public ExpressionLookBack getLookBack() {
                        return ExpressionLookBack.EMPTY;
                    }

                    @Override
                    public boolean test(Context ctx) {
                        return is_tag_group1.apply(ctx)
                                .streamValues()
                                .map(MetricValue::asBool)
                                .flatMap((Optional<Boolean> m) -> m.map(Stream::of).orElseGet(Stream::empty))
                                .anyMatch((Boolean bool) -> bool != false);
                    }
                }),
                Stream.of(group_rule));

        matchStmt.transform(ctx);

        Mockito.verify(group1, Mockito.atLeastOnce()).getGroup();
        Mockito.verify(group2, Mockito.atLeastOnce()).getGroup();
        Mockito.verify(group3, Mockito.atLeastOnce()).getGroup();
        Mockito.verify(group1, Mockito.times(1)).findMetric(METRIC);
        Mockito.verify(group2, Mockito.times(1)).findMetric(METRIC);
        Mockito.verify(group3, Mockito.times(0)).findMetric(METRIC);
        assertEquals(2, group_rule.getInvocations());
    }

    @Test
    public void match_statement_once_each_using_metric_matcher() {
        final MatchTransformerImpl matchStmt = new MatchTransformerImpl(
                EMPTY_MAP,
                singletonMap(new MatchStatement.IdentifierPair(IDENT, METRIC_IDENT), new MetricMatcher(wildcard_matcher, wildcard_matcher)),
                Optional.empty(),
                Stream.of(metric_ident_rule));

        matchStmt.transform(ctx);

        Mockito.verify(group1, Mockito.atLeastOnce()).getGroup();
        Mockito.verify(group2, Mockito.atLeastOnce()).getGroup();
        Mockito.verify(group3, Mockito.atLeastOnce()).getGroup();
        Mockito.verify(group1, Mockito.atLeastOnce()).findMetric(METRIC);
        Mockito.verify(group2, Mockito.atLeastOnce()).findMetric(METRIC);
        Mockito.verify(group3, Mockito.atLeastOnce()).findMetric(METRIC);
        assertEquals(3, metric_ident_rule.getInvocations());
    }
}
