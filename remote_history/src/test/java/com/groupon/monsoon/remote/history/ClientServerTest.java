/*
 * Copyright (c) 2016, Ariane van der Steldt
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
package com.groupon.monsoon.remote.history;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.ImmutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.SimpleTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.io.IOException;
import java.net.Inet4Address;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.reverse;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.OncRpcProtocols;
import org.acplt.oncrpc.server.OncRpcServerTransport;
import org.acplt.oncrpc.server.OncRpcUdpServerTransport;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClientServerTest {
    private static final int COUNT = 100;
    private static final Logger LOG = Logger.getLogger(ClientServerTest.class.getName());
    private static final DateTime T0 = new DateTime(DateTimeZone.UTC);
    private static final GroupName TEST_GROUP = GroupName.valueOf(SimpleGroupPath.valueOf("test", "group"), Tags.valueOf(singletonMap("x", MetricValue.fromStrValue("x"))));

    @Mock
    private CollectHistory history;

    private CollectHistoryServer server;
    private Client client;
    private TimeSeriesMetricExpression expr;

    @Before
    public void setup() throws Exception {
        expr = TimeSeriesMetricExpression.valueOf("1 + 2");
        CompletableFuture<Integer> portFuture = new CompletableFuture<>();

        server = new CollectHistoryServer(history);
        final Thread serverThread = new Thread(() -> {
            OncRpcServerTransport transport = null;
            try {
                transport = new OncRpcUdpServerTransport(server, Inet4Address.getLoopbackAddress(), 0, server.info, 32768);
                transport.setCharacterEncoding("UTF-8");
                portFuture.complete(transport.getPort());
                server.run(new OncRpcServerTransport[]{transport});
            } catch (IOException | OncRpcException ex) {
                LOG.log(Level.SEVERE, "server " + server + " failed to start", ex);
                portFuture.completeExceptionally(ex);
            } finally {
                if (transport != null) transport.close();
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
        final int port = portFuture.get(15, TimeUnit.SECONDS);
        LOG.log(Level.INFO, "chosen port: {0}", port);

        LOG.log(Level.INFO, "started server {0}", server);
        client = new Client(Inet4Address.getLoopbackAddress(), port, OncRpcProtocols.ONCRPC_UDP);
        LOG.log(Level.INFO, "started client {0}", client);
    }

    @After
    public void cleanup() throws Exception {
        server.stopRpcProcessing();
        LOG.log(Level.INFO, "stopped server {0}", server);
    }

    @Test
    public void getFileSize() throws Exception {
        when(history.getFileSize()).thenReturn(100000l);

        long fileSize = client.getFileSize();

        assertEquals(100000, fileSize);

        verify(history, times(1)).getFileSize();
        verifyNoMoreInteractions(history);
    }

    @Test
    public void getEnd() throws Exception {
        when(history.getEnd()).thenReturn(T0);

        DateTime end = client.getEnd();

        assertEquals(T0, end);

        verify(history, times(1)).getEnd();
        verifyNoMoreInteractions(history);
    }

    @Test
    public void add() throws Exception {
        // Invokes the addAll method.
        List<TimeSeriesCollection> expected = generateCollection().limit(1).collect(Collectors.toList());
        when(history.addAll(Mockito.<Collection<? extends TimeSeriesCollection>>any())).thenReturn(true);

        client.add(expected.get(0));

        verify(history, times(1)).addAll(Mockito.eq(expected));
        verifyNoMoreInteractions(history);
    }

    @Test
    public void addAll() throws Exception {
        List<TimeSeriesCollection> expected = generateCollection().collect(Collectors.toList());
        when(history.addAll(Mockito.<Collection<? extends TimeSeriesCollection>>any())).thenReturn(true);

        client.addAll(expected);

        verify(history, times(1)).addAll(Mockito.eq(expected));
        verifyNoMoreInteractions(history);
    }

    @Test
    public void streamReversed() {
        final List<TimeSeriesCollection> expected = generateCollection().collect(Collectors.toList());
        reverse(expected);
        when(history.streamReversed())
                .thenAnswer((invocation) -> expected.stream());

        final List<TimeSeriesCollection> result = client.streamReversed().collect(Collectors.toList());

        assertEquals(expected, result);

        verify(history, times(1)).streamReversed();
        verifyNoMoreInteractions(history);
    }

    @Test
    public void stream() {
        final List<TimeSeriesCollection> expected = generateCollection().collect(Collectors.toList());
        when(history.stream())
                .thenAnswer((invocation) -> generateCollection());

        final List<TimeSeriesCollection> result = client.stream().collect(Collectors.toList());

        assertEquals(expected, result);

        verify(history, times(1)).stream();
        verifyNoMoreInteractions(history);
    }

    @Test
    public void streamGroup() {
        final List<Map.Entry<DateTime, TimeSeriesValue>> expected = generateCollection()
                .map(tsc -> SimpleMapEntry.create(tsc.getTimestamp(), tsc.get(TEST_GROUP).get()))
                .collect(Collectors.toList());
        when(history.streamGroup(Mockito.isA(DateTime.class), Mockito.any()))
                .thenAnswer((invocation) -> expected.stream());

        final List<Map.Entry<DateTime, TimeSeriesValue>> result = client.streamGroup(T0, TEST_GROUP).collect(Collectors.toList());

        assertEquals(expected, result);

        verify(history, times(1)).streamGroup(Mockito.eq(T0), Mockito.eq(TEST_GROUP));
        verifyNoMoreInteractions(history);
    }

    @Test
    public void streamFrom() {
        final DateTime begin = new DateTime(DateTimeZone.UTC);
        final List<TimeSeriesCollection> expected = generateCollection().collect(Collectors.toList());
        when(history.stream(Mockito.isA(DateTime.class)))
                .thenAnswer((invocation) -> generateCollection());

        final List<TimeSeriesCollection> result = client.stream(begin).collect(Collectors.toList());

        assertEquals(expected, result);

        verify(history, times(1)).stream(Mockito.eq(begin));
        verifyNoMoreInteractions(history);
    }

    @Test
    public void streamFromTo() {
        final DateTime end = new DateTime(DateTimeZone.UTC);
        final DateTime begin = end.minus(Duration.standardDays(1));
        final List<TimeSeriesCollection> expected = generateCollection().collect(Collectors.toList());
        when(history.stream(Mockito.isA(DateTime.class), Mockito.isA(DateTime.class)))
                .thenAnswer((invocation) -> generateCollection());

        final List<TimeSeriesCollection> result = client.stream(begin, end).collect(Collectors.toList());

        assertEquals(expected, result);

        verify(history, times(1)).stream(Mockito.eq(begin), Mockito.eq(end));
        verifyNoMoreInteractions(history);
    }

    @Test
    public void streamStepped() {
        final Duration stepSize = Duration.standardMinutes(1);
        final List<TimeSeriesCollection> expected = generateCollection().collect(Collectors.toList());
        when(history.stream(Mockito.isA(Duration.class)))
                .thenAnswer((invocation) -> generateCollection());

        final List<TimeSeriesCollection> result = client.stream(stepSize).collect(Collectors.toList());

        assertEquals(expected, result);

        verify(history, times(1)).stream(Mockito.eq(stepSize));
        verifyNoMoreInteractions(history);
    }

    @Test
    public void streamSteppedFrom() {
        final DateTime begin = new DateTime(DateTimeZone.UTC);
        final Duration stepSize = Duration.standardMinutes(1);
        final List<TimeSeriesCollection> expected = generateCollection().collect(Collectors.toList());
        when(history.stream(Mockito.isA(DateTime.class), Mockito.isA(Duration.class)))
                .thenAnswer((invocation) -> generateCollection());

        final List<TimeSeriesCollection> result = client.stream(begin, stepSize).collect(Collectors.toList());

        assertEquals(expected, result);

        verify(history, times(1)).stream(Mockito.eq(begin), Mockito.eq(stepSize));
        verifyNoMoreInteractions(history);
    }

    @Test
    public void streamSteppedFromTo() {
        final DateTime end = new DateTime(DateTimeZone.UTC);
        final DateTime begin = end.minus(Duration.standardDays(1));
        final Duration stepSize = Duration.standardMinutes(1);
        final List<TimeSeriesCollection> expected = generateCollection().collect(Collectors.toList());
        when(history.stream(Mockito.isA(DateTime.class), Mockito.isA(DateTime.class), Mockito.isA(Duration.class)))
                .thenAnswer((invocation) -> generateCollection());

        final List<TimeSeriesCollection> result = client.stream(begin, end, stepSize).collect(Collectors.toList());

        assertEquals(expected, result);

        verify(history, times(1)).stream(Mockito.eq(begin), Mockito.eq(end), Mockito.eq(stepSize));
        verifyNoMoreInteractions(history);
    }

    @Test
    public void evaluate() {
        final Duration stepSize = Duration.standardMinutes(1);
        final List<Collection<CollectHistory.NamedEvaluation>> expected = generateEvaluation().collect(Collectors.toList());
        when(history.evaluate(Mockito.any(), Mockito.isA(Duration.class)))
                .thenAnswer((invocation) -> generateEvaluation());

        final List<Collection<CollectHistory.NamedEvaluation>> result = client.evaluate(singletonMap("name", expr), stepSize).collect(Collectors.toList());

        assertEquals(expected, result);

        verify(history, times(1)).evaluate(
                Mockito.<Map>argThat(
                        Matchers.hasEntry(
                                Matchers.equalTo("name"),
                                new ExprEqualTo(expr))),
                Mockito.eq(stepSize));
        verifyNoMoreInteractions(history);
    }

    @Test
    public void evaluateFrom() {
        final DateTime begin = new DateTime(DateTimeZone.UTC);
        final Duration stepSize = Duration.standardMinutes(1);
        final List<Collection<CollectHistory.NamedEvaluation>> expected = generateEvaluation().collect(Collectors.toList());
        when(history.evaluate(Mockito.any(), Mockito.isA(DateTime.class), Mockito.isA(Duration.class)))
                .thenAnswer((invocation) -> generateEvaluation());

        final List<Collection<CollectHistory.NamedEvaluation>> result = client.evaluate(singletonMap("name", expr), begin, stepSize).collect(Collectors.toList());

        assertEquals(expected, result);

        verify(history, times(1)).evaluate(
                Mockito.<Map>argThat(
                        Matchers.hasEntry(
                                Matchers.equalTo("name"),
                                new ExprEqualTo(expr))),
                Mockito.eq(begin),
                Mockito.eq(stepSize));
        verifyNoMoreInteractions(history);
    }

    @Test
    public void evaluateFromTo() {
        final DateTime end = new DateTime(DateTimeZone.UTC);
        final DateTime begin = end.minus(Duration.standardDays(1));
        final Duration stepSize = Duration.standardMinutes(1);
        final List<Collection<CollectHistory.NamedEvaluation>> expected = generateEvaluation().collect(Collectors.toList());
        when(history.evaluate(Mockito.any(), Mockito.isA(DateTime.class), Mockito.isA(DateTime.class), Mockito.isA(Duration.class)))
                .thenAnswer((invocation) -> generateEvaluation());

        final List<Collection<CollectHistory.NamedEvaluation>> result = client.evaluate(singletonMap("name", expr), begin, end, stepSize).collect(Collectors.toList());

        assertEquals(expected, result);

        verify(history, times(1)).evaluate(
                Mockito.<Map>argThat(
                        Matchers.hasEntry(
                                Matchers.equalTo("name"),
                                new ExprEqualTo(expr))),
                Mockito.eq(begin),
                Mockito.eq(end),
                Mockito.eq(stepSize));
        verifyNoMoreInteractions(history);
    }

    /**
     * Test encoding/decoding using a specific metric value. The test uses the
     * StreamGroup method and puts the metric value in both the requested group
     * name and the returned response.
     *
     * This ensures both literal and dictionary-based encoding of the metric
     * value is tested.
     *
     * @param metricValue The metric value to test encoding/decoding on.
     */
    private void typedEncodingDecodingTest(MetricValue metricValue) {
        final GroupName testGroup;
        if (!metricValue.isPresent() || metricValue.getHistValue() != null)
            testGroup = TEST_GROUP; // Empty and histogram are not valid in tags.
        else
            testGroup = GroupName.valueOf(SimpleGroupPath.valueOf("x", "y"), Tags.valueOf(singletonMap("z", metricValue)));

        final List<Map.Entry<DateTime, TimeSeriesValue>> expected = singletonList(SimpleMapEntry.create(T0, new ImmutableTimeSeriesValue(testGroup, singletonMap(MetricName.valueOf("foo"), metricValue))));
        when(history.streamGroup(Mockito.isA(DateTime.class), Mockito.any()))
                .thenAnswer((invocation) -> expected.stream());

        final List<Map.Entry<DateTime, TimeSeriesValue>> result = client.streamGroup(T0, testGroup).collect(Collectors.toList());

        assertEquals(expected, result);
        verify(history, times(1)).streamGroup(Mockito.eq(T0), Mockito.eq(testGroup));
    }

    @Test
    public void typedEncodingDecodingEmpty() {
        typedEncodingDecodingTest(MetricValue.EMPTY);
    }

    @Test
    public void typedEncodingDecodingTrue() {
        typedEncodingDecodingTest(MetricValue.TRUE);
    }

    @Test
    public void typedEncodingDecodingFalse() {
        typedEncodingDecodingTest(MetricValue.TRUE);
    }

    @Test
    public void typedEncodingDecodingInt() {
        typedEncodingDecodingTest(MetricValue.fromIntValue(19));
    }

    @Test
    public void typedEncodingDecodingFloat() {
        typedEncodingDecodingTest(MetricValue.fromDblValue(Math.E));
    }

    @Test
    public void typedEncodingDecodingString() {
        typedEncodingDecodingTest(MetricValue.fromStrValue("chocoladevla"));
    }

    @Test
    public void typedEncodingDecodingHistogram() {
        typedEncodingDecodingTest(MetricValue.fromHistValue(new Histogram(new Histogram.RangeWithCount(0.1, 0.9, 7.3), new Histogram.RangeWithCount(5.9, 5.98, Math.PI))));
    }

    // Triggers the min-timeout durations.
    @Test(timeout = 20000)
    public void slowEvaluate() {
        final int LIMIT = 10;  // To make the test not take forever.
        final Duration stepSize = Duration.standardMinutes(1);
        final List<Collection<CollectHistory.NamedEvaluation>> expected = generateEvaluation().limit(LIMIT).collect(Collectors.toList());
        when(history.evaluate(Mockito.any(), Mockito.isA(Duration.class)))
                .thenAnswer((invocation) -> {
                    return generateEvaluation()
                            .limit(LIMIT)
                            .map(x -> {
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException ex) {
                                    Logger.getLogger(ClientServerTest.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                Logger.getLogger(ClientServerTest.class.getName()).log(Level.INFO, "emitting {0}", x);
                                return x;
                            });
                });

        final List<Collection<CollectHistory.NamedEvaluation>> result = client.evaluate(singletonMap("name", expr), stepSize).collect(Collectors.toList());

        assertEquals(expected, result);

        verify(history, times(1)).evaluate(
                Mockito.<Map>argThat(
                        Matchers.hasEntry(
                                Matchers.equalTo("name"),
                                new ExprEqualTo(expr))),
                Mockito.eq(stepSize));
        verifyNoMoreInteractions(history);
    }

    // Triggers the max timeout durations.
    @Test(timeout = 20000)
    public void verySlowEvaluate() {
        final int LIMIT = 1;  // To make the test not take forever.
        final Duration stepSize = Duration.standardMinutes(1);
        final List<Collection<CollectHistory.NamedEvaluation>> expected = generateEvaluation().limit(LIMIT).collect(Collectors.toList());
        when(history.evaluate(Mockito.any(), Mockito.isA(Duration.class)))
                .thenAnswer((invocation) -> {
                    return generateEvaluation()
                            .limit(LIMIT)
                            .map(x -> {
                                try {
                                    Thread.sleep(10000);
                                } catch (InterruptedException ex) {
                                    Logger.getLogger(ClientServerTest.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                Logger.getLogger(ClientServerTest.class.getName()).log(Level.INFO, "emitting {0}", x);
                                return x;
                            });
                });

        final List<Collection<CollectHistory.NamedEvaluation>> result = client.evaluate(singletonMap("name", expr), stepSize).collect(Collectors.toList());

        assertEquals(expected, result);

        verify(history, times(1)).evaluate(
                Mockito.<Map>argThat(
                        Matchers.hasEntry(
                                Matchers.equalTo("name"),
                                new ExprEqualTo(expr))),
                Mockito.eq(stepSize));
        verifyNoMoreInteractions(history);
    }

    private static Stream<Integer> generate() {
        class Generator implements Supplier<Integer> {
            private int i;

            @Override
            public Integer get() {
                return i++;
            }
        }

        return Stream.generate(new Generator());
    }

    private static Stream<TimeSeriesCollection> generateCollection() {
        final MetricName metricName = MetricName.valueOf("counter");

        return generate()
                .limit(COUNT)
                .map(i -> {
                    final DateTime t = T0.plus(Duration.standardSeconds(10 * i));
                    final MetricValue counter = MetricValue.fromIntValue(i);
                    return new SimpleTimeSeriesCollection(t, Stream.of(new ImmutableTimeSeriesValue(TEST_GROUP, singletonMap(metricName, counter))));
                });
    }

    private static Stream<Collection<CollectHistory.NamedEvaluation>> generateEvaluation() {
        return generate()
                .limit(COUNT)
                .map(i -> {
                    final DateTime t = T0.plus(Duration.standardSeconds(10 * i));
                    return new CollectHistory.NamedEvaluation("name", t, new TimeSeriesMetricDeltaSet(MetricValue.fromIntValue(i)));
                })
                .map(Collections::singletonList);
    }

    @RequiredArgsConstructor
    private static class ExprEqualTo<T> extends BaseMatcher<T> {
        private final TimeSeriesMetricExpression expr;

        @Override
        public boolean matches(Object item) {
            if (!(item instanceof TimeSeriesMetricExpression)) return false;
            TimeSeriesMetricExpression itemExpr = (TimeSeriesMetricExpression) item;
            LOG.log(Level.FINE, "comparing {0} with {1}", new Object[]{expr.configString(), itemExpr.configString()});
            return Objects.equals(expr.configString().toString(), itemExpr.configString().toString());
        }

        @Override
        public void describeTo(Description description) {
            description.appendValue(expr);
        }
    }
}
