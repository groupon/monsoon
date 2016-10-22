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
package com.groupon.lex.metrics;

import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPairInstance;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.io.IOException;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.hamcrest.Matchers.hasItem;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

/**
 *
 * @author ariane
 */
@RunWith(MockitoJUnitRunner.class)
public class MetricRegistryInstanceTest {
    @Mock
    private GroupGenerator generator, extra_generator;
    @Mock
    private MetricRegistryInstance.CollectionContext cctx;

    private MetricRegistryInstance create(boolean has_config) {
        return new MetricRegistryInstance(has_config, (pattern, handler) -> {
        }) {
            @Override
            protected MetricRegistryInstance.CollectionContext beginCollection(DateTime now) {
                when(cctx.alertManager()).thenReturn((Alert alert) -> {
                });
                when(cctx.tsdata()).thenAnswer(new Answer<TimeSeriesCollectionPair>() {
                    @Override
                    public TimeSeriesCollectionPair answer(InvocationOnMock invocation) throws Throwable {
                        return new TimeSeriesCollectionPairInstance(now);
                    }
                });

                return cctx;
            }
        };
    }

    private MetricRegistryInstance create(boolean has_config, DateTime now) {
        return new MetricRegistryInstance(() -> now, has_config, (pattern, handler) -> {
        }) {
            @Override
            protected MetricRegistryInstance.CollectionContext beginCollection(DateTime now) {
                when(cctx.alertManager()).thenReturn((Alert alert) -> {
                });
                when(cctx.tsdata()).thenAnswer(new Answer<TimeSeriesCollectionPair>() {
                    @Override
                    public TimeSeriesCollectionPair answer(InvocationOnMock invocation) throws Throwable {
                        return new TimeSeriesCollectionPairInstance(now);
                    }
                });

                return cctx;
            }
        };
    }

    @Test
    public void constructor() {
        try (MetricRegistryInstance mr = create(true)) {
            assertTrue(mr.hasConfig());

            assertEquals(Optional.empty(), mr.getRuleEvalDuration());
            assertEquals(Optional.empty(), mr.getProcessorDuration());
        }

        try (MetricRegistryInstance mr = create(false)) {
            assertFalse(mr.hasConfig());

            assertEquals(Optional.empty(), mr.getRuleEvalDuration());
            assertEquals(Optional.empty(), mr.getProcessorDuration());
        }
    }

    @Test
    public void processor_duration_is_remembered() {
        try (MetricRegistryInstance mr = create(false)) {
            mr.updateProcessorDuration(Duration.standardSeconds(17));
            assertEquals(Optional.of(Duration.standardSeconds(17)), mr.getProcessorDuration());
        }
    }

    @Test
    public void generator_handling() throws Exception {
        when(generator.getGroups(Mockito.any(), Mockito.any()))
                .thenReturn(singleton(CompletableFuture.completedFuture(singleton(new SimpleMetricGroup(GroupName.valueOf("test"), Stream.of(new SimpleMetric(MetricName.valueOf("x"), 17)))))));
        final DateTime now = DateTime.now(DateTimeZone.UTC);

        try (MetricRegistryInstance mr = create(false, now)) {
            mr.add(generator);
            List<TimeSeriesValue> sgroups = mr.updateCollection().getTSValues().stream().collect(Collectors.toList());

            assertThat(sgroups,
                    hasItem(new MutableTimeSeriesValue(now, GroupName.valueOf("test"), singletonMap(MetricName.valueOf("x"), MetricValue.fromIntValue(17)))));
        }

        verify(generator, times(1)).getGroups(Mockito.any(), Mockito.any());
        verify(generator, times(1)).close();
    }

    @Test
    public void collectionContext_handling() throws Exception {
        try (MetricRegistryInstance mr = create(false)) {
            mr.updateCollection();
        }

        verify(cctx, times(1)).alertManager();
        verify(cctx, times(1)).tsdata();
        verify(cctx, times(1)).commit();
        verifyNoMoreInteractions(cctx);
    }

    @Test
    public void stream_groups() throws Exception {
        when(generator.getGroups(Mockito.any(), Mockito.any()))
                .thenReturn(singleton(CompletableFuture.completedFuture(singleton(new SimpleMetricGroup(GroupName.valueOf("test"), Stream.of(new SimpleMetric(MetricName.valueOf("x"), 17)))))));

        try (MetricRegistryInstance mr = create(false)) {
            mr.add(generator);
            mr.updateCollection();
        }

        verify(generator, times(1)).getGroups(Mockito.any(), Mockito.any());
        verify(generator, times(1)).close();
        verifyNoMoreInteractions(generator);
    }

    @Test
    public void ignore_close_errors() throws Exception {
        Mockito.doThrow(IOException.class).when(generator).close();
        Mockito.doThrow(IOException.class).when(extra_generator).close();

        try (MetricRegistryInstance mr = create(false)) {
            mr.add(generator);
            mr.add(extra_generator);
        }

        verify(generator, times(1)).close();
        verify(generator, times(1)).close();
        verifyNoMoreInteractions(generator);
    }
}
