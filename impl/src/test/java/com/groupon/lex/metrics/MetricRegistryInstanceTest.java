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

import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.io.IOException;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.hamcrest.Matchers.arrayContaining;
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
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * @author ariane
 */
@RunWith(MockitoJUnitRunner.class)
public class MetricRegistryInstanceTest {
    @Mock
    private GroupGenerator generator, extra_generator;

    @Test
    public void constructor() {
        try (MetricRegistryInstance mr = MetricRegistryInstance.create(true, (pattern, servlet) -> {})) {
            assertTrue(mr.hasConfig());

            assertEquals(Optional.empty(), mr.getRuleEvalDuration());
            assertEquals(Optional.empty(), mr.getProcessorDuration());
        }

        try (MetricRegistryInstance mr = MetricRegistryInstance.create(false, (pattern, servlet) -> {})) {
            assertFalse(mr.hasConfig());

            assertEquals(Optional.empty(), mr.getRuleEvalDuration());
            assertEquals(Optional.empty(), mr.getProcessorDuration());
        }
    }

    @Test
    public void processor_duration_is_remembered() {
        try (MetricRegistryInstance mr = MetricRegistryInstance.create(false, (pattern, servlet) -> {})) {
            mr.updateProcessorDuration(Duration.standardSeconds(17));
            assertEquals(Optional.of(Duration.standardSeconds(17)), mr.getProcessorDuration());
        }
    }

    @Test
    public void generator_handling() throws Exception {
        when(generator.getGroups()).thenReturn(GroupGenerator.successResult(singleton(new SimpleMetricGroup(new GroupName("test"), Stream.of(new SimpleMetric(new MetricName("x"), 17))))));
        final DateTime now = DateTime.now(DateTimeZone.UTC);

        try (MetricRegistryInstance mr = MetricRegistryInstance.create(false, (pattern, servlet) -> {})) {
            mr.add(generator);
            List<TimeSeriesValue> sgroups = mr.streamGroups(now).collect(Collectors.toList());

            assertThat(sgroups,
                    hasItem(new MutableTimeSeriesValue(now, new GroupName("test"), singletonMap(new MetricName("x"), MetricValue.fromIntValue(17)))));
        }

        verify(generator, times(1)).getGroups();
        verify(generator, times(1)).close();
    }

    @Test
    public void stream_groups() throws Exception {
        when(generator.getGroups()).thenReturn(GroupGenerator.successResult(singleton(new SimpleMetricGroup(new GroupName("test"), Stream.of(new SimpleMetric(new MetricName("x"), 17))))));

        try (MetricRegistryInstance mr = MetricRegistryInstance.create(false, (pattern, servlet) -> {})) {
            mr.add(generator);
            mr.streamGroups().collect(Collectors.toList());
        }

        verify(generator, times(1)).getGroups();
        verify(generator, times(1)).close();
    }

    @Test
    public void group_names_resolution() throws Exception {
        when(generator.getGroups()).thenReturn(GroupGenerator.successResult(singleton(new SimpleMetricGroup(new GroupName("test"), Stream.of(new SimpleMetric(new MetricName("x"), 17))))));

        try (MetricRegistryInstance mr = MetricRegistryInstance.create(false, (pattern, servlet) -> {})) {
            mr.add(generator);
            assertThat(mr.getGroupNames(),
                    arrayContaining(new GroupName("test")));
        }
    }

    @Test
    public void ignore_close_errors() throws Exception {
        Mockito.doThrow(IOException.class).when(generator).close();
        Mockito.doThrow(IOException.class).when(extra_generator).close();

        try (MetricRegistryInstance mr = MetricRegistryInstance.create(false, (pattern, servlet) -> {})) {
            mr.add(generator);
            mr.add(extra_generator);
        }

        verify(generator, times(1)).close();
        verify(generator, times(1)).close();
    }
}
