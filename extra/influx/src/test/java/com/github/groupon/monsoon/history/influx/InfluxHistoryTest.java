/*
 * Copyright (c) 2017, ariane
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.groupon.monsoon.history.influx;

import com.groupon.lex.metrics.MetricMatcher;
import com.groupon.lex.metrics.PathMatcher;
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class InfluxHistoryTest {
    private static final Logger LOG = Logger.getLogger(InfluxHistoryTest.class.getName());
    private static final String DATABASE = "database";

    @Mock
    private InfluxDB influxDB;

    private InfluxHistory history;

    @Before
    public void setup() {
        Mockito.when(influxDB.databaseExists(Mockito.any())).thenReturn(true);

        history = new InfluxHistory(influxDB, DATABASE);

        Mockito.verify(influxDB, Mockito.atMost(1)).databaseExists(Mockito.eq(DATABASE));
    }

    @Test
    public void closePropagates() {
        history.close();

        verify(influxDB, times(1)).close();
        verifyNoMoreInteractions(influxDB);
    }

    @Test
    public void getEnd() throws Exception {
        Mockito.when(influxDB.query(Mockito.any(), Mockito.any()))
                .thenReturn(new JsonQueryResult("InfluxHistory_getEnd").getQueryResult());

        assertThat(history.getEnd(), Matchers.equalTo(new DateTime(1505665450000L, DateTimeZone.UTC)));

        verify(influxDB, times(1)).query(
                Mockito.argThat(
                        Matchers.allOf(
                                Matchers.hasProperty("command", Matchers.equalToIgnoringCase("select * from /.*/ order by time desc limit 1")),
                                Matchers.hasProperty("database", Matchers.equalTo(DATABASE)))),
                Mockito.eq(TimeUnit.MILLISECONDS)
        );
        verifyNoMoreInteractions(influxDB);
    }

    @Test
    public void getBegin() throws Exception {
        Mockito.when(influxDB.query(Mockito.any(), Mockito.any()))
                .thenReturn(new JsonQueryResult("InfluxHistory_getBegin").getQueryResult());

        assertThat(history.getBegin(), Matchers.equalTo(Optional.of(new DateTime(1505241470000L, DateTimeZone.UTC))));

        verify(influxDB, times(1)).query(
                Mockito.argThat(
                        Matchers.allOf(
                                Matchers.hasProperty("command", Matchers.equalToIgnoringCase("select * from /.*/ order by time asc limit 1")),
                                Matchers.hasProperty("database", Matchers.equalTo(DATABASE)))),
                Mockito.eq(TimeUnit.MILLISECONDS)
        );
        verifyNoMoreInteractions(influxDB);
    }

    @Test
    public void getFileSize() throws Exception {
        Mockito.when(influxDB.query(Mockito.any(), Mockito.any()))
                .thenReturn(new JsonQueryResult("InfluxHistory_getFileSize").getQueryResult());

        assertEquals(12185178L, history.getFileSize());

        verify(influxDB, times(1)).query(
                Mockito.argThat(
                        Matchers.allOf(
                                Matchers.hasProperty("command", Matchers.equalToIgnoringCase("select sum(\"diskBytes\") as \"diskBytes\" from (select last(\"diskBytes\"::field) as \"diskBytes\" from \"shard\" where \"database\"::tag = '" + DATABASE + "' group by * limit 1)")),
                                Matchers.hasProperty("database", Matchers.equalTo("_internal")))),
                Mockito.eq(TimeUnit.MILLISECONDS)
        );
        verifyNoMoreInteractions(influxDB);
    }

    @Test
    public void streamReverse() throws Exception {
        Mockito
                .when(influxDB.query(Mockito.any(), Mockito.any()))
                .thenAnswer(keyedQueriesAnswer(STREAM_REVERSE_QUERIES));

        assertThat(history.streamReversed().collect(Collectors.toList()),
                computeDataMatcher(STREAM_REVERSE_QUERIES, true));

        // The end query was called once.
        verify(influxDB, times(STREAM_REVERSE_QUERIES.size())).query(
                Mockito.argThat(Matchers.hasProperty("database", Matchers.equalTo(DATABASE))),
                Mockito.eq(TimeUnit.MILLISECONDS)
        );
        verifyNoMoreInteractions(influxDB);
    }

    @Test
    public void stream() throws Exception {
        Mockito
                .when(influxDB.query(Mockito.any(), Mockito.any()))
                .thenAnswer(keyedQueriesAnswer(STREAM_QUERIES));

        assertThat(history.stream().collect(Collectors.toList()),
                computeDataMatcher(STREAM_QUERIES, false));

        // The end query was called once.
        verify(influxDB, times(STREAM_QUERIES.size())).query(
                Mockito.argThat(Matchers.hasProperty("database", Matchers.equalTo(DATABASE))),
                Mockito.eq(TimeUnit.MILLISECONDS)
        );
        verifyNoMoreInteractions(influxDB);
    }

    @Test
    public void streamWithFilter() throws Exception {
        final TimeSeriesMetricFilter filter = new TimeSeriesMetricFilter()
                .withMetric(new MetricMatcher(
                        new PathMatcher(new PathMatcher.LiteralNameMatch("runtime")),
                        new PathMatcher(new PathMatcher.LiteralNameMatch("Mallocs"))));

        Mockito
                .when(influxDB.query(Mockito.any(), Mockito.any()))
                .thenAnswer(keyedQueriesAnswer(STREAM_WITH_FILTER_QUERIES));

        assertThat(history.stream(filter).collect(Collectors.toList()),
                computeDataMatcher(STREAM_WITH_FILTER_QUERIES, false));

        // The end query was called once.
        verify(influxDB, times(STREAM_WITH_FILTER_QUERIES.size())).query(
                Mockito.argThat(Matchers.hasProperty("database", Matchers.equalTo(DATABASE))),
                Mockito.eq(TimeUnit.MILLISECONDS)
        );
        verifyNoMoreInteractions(influxDB);
    }

    @Test
    public void streamWithBegin() throws Exception {
        Mockito
                .when(influxDB.query(Mockito.any(), Mockito.any()))
                .thenAnswer(keyedQueriesAnswer(STREAM_WITH_BEGIN_QUERIES));

        assertThat(history.stream(DateTime.parse("2017-09-17T16:00:00.000Z")).collect(Collectors.toList()),
                computeDataMatcher(STREAM_WITH_BEGIN_QUERIES, false));

        // The end query was called once.
        verify(influxDB, times(STREAM_WITH_BEGIN_QUERIES.size())).query(
                Mockito.argThat(Matchers.hasProperty("database", Matchers.equalTo(DATABASE))),
                Mockito.eq(TimeUnit.MILLISECONDS)
        );
        verifyNoMoreInteractions(influxDB);
    }

    @Test
    public void streamWithBeginAndEnd() throws Exception {
        Mockito
                .when(influxDB.query(Mockito.any(), Mockito.any()))
                .thenAnswer(keyedQueriesAnswer(STREAM_WITH_BEGIN_AND_END_QUERIES));

        assertThat(history.stream(DateTime.parse("2017-09-17T10:00:00.000Z"), DateTime.parse("2017-09-17T14:00:00.000Z")).collect(Collectors.toList()),
                computeDataMatcher(STREAM_WITH_BEGIN_AND_END_QUERIES, false));

        // The end query was called once.
        verify(influxDB, times(STREAM_WITH_BEGIN_AND_END_QUERIES.size())).query(
                Mockito.argThat(Matchers.hasProperty("database", Matchers.equalTo(DATABASE))),
                Mockito.eq(TimeUnit.MILLISECONDS)
        );
        verifyNoMoreInteractions(influxDB);
    }

    @Test
    public void evaluateWithBeginAndEnd() throws Exception {
        Mockito
                .when(influxDB.query(Mockito.any(), Mockito.any()))
                .thenAnswer(keyedQueriesAnswer(EVAL_WITH_BEGIN_AND_END_QUERIES));

        List<Collection<CollectHistory.NamedEvaluation>> expected = mockHistoryFromData(computeData(EVAL_WITH_BEGIN_AND_END_QUERIES)).evaluate(singletonMap("foobar", TimeSeriesMetricExpression.valueOf("rate[5m](runtime NumGC)")), DateTime.parse("2017-09-17T10:00:00.000Z"), DateTime.parse("2017-09-17T14:00:00.000Z"), Duration.millis(1)).collect(Collectors.toList());
        assertEquals(expected, history.evaluate(singletonMap("foobar", TimeSeriesMetricExpression.valueOf("rate[5m](runtime NumGC)")), DateTime.parse("2017-09-17T10:00:00.000Z"), DateTime.parse("2017-09-17T14:00:00.000Z"), Duration.millis(1)).collect(Collectors.toList()));

        // The end query was called once.
        verify(influxDB, times(EVAL_WITH_BEGIN_AND_END_QUERIES.size())).query(
                Mockito.argThat(Matchers.hasProperty("database", Matchers.equalTo(DATABASE))),
                Mockito.eq(TimeUnit.MILLISECONDS)
        );
        verifyNoMoreInteractions(influxDB);
    }

    private static final List<KeyedQuery> STREAM_REVERSE_QUERIES = unmodifiableList(Arrays.asList(
            new KeyedQuery("select * from /.*/ order by time desc limit 1", "InfluxHistory_getEnd", false),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T15:24:10.000Z' and time <= '2017-09-17T16:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_1", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T14:24:10.000Z' and time <= '2017-09-17T15:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_2", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T13:24:10.000Z' and time <= '2017-09-17T14:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_3", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T12:24:10.000Z' and time <= '2017-09-17T13:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_4", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T11:24:10.000Z' and time <= '2017-09-17T12:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_5", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T10:24:10.000Z' and time <= '2017-09-17T11:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_6", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T09:24:10.000Z' and time <= '2017-09-17T10:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_7", true),
            new KeyedQuery("select * from /.*/ where time <= '2017-09-17T09:24:10.000Z' order by time desc limit 1", "InfluxHistory_streamReverse_skipQuery", false),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-12T17:39:30.000Z' and time <= '2017-09-12T18:39:30.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_8_afterSkipQuery", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-12T16:39:30.000Z' and time <= '2017-09-12T17:39:30.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_9_afterSkipQuery", true),
            new KeyedQuery("select * from /.*/ where time <= '2017-09-12T16:39:30.000Z' order by time desc limit 1", "InfluxHistory_streamReverse_TheEnd", false)
    ));

    private static final List<KeyedQuery> STREAM_QUERIES = unmodifiableList(Arrays.asList(
            new KeyedQuery("select * from /.*/ order by time desc limit 1", "InfluxHistory_getEnd", false),
            new KeyedQuery("select * from /.*/ order by time asc limit 1", "InfluxHistory_getBegin", false),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-12T18:37:49.999Z' and time <= '2017-09-12T19:37:49.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_stream_1", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-12T19:37:49.999Z' and time <= '2017-09-12T20:37:49.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_stream_2", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-12T20:37:49.999Z' and time <= '2017-09-12T21:37:49.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_stream_3", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-12T21:37:49.999Z' and time <= '2017-09-12T22:37:49.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_stream_4", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-12T22:37:49.999Z' and time <= '2017-09-12T23:37:49.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_stream_5", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-12T23:37:49.999Z' and time <= '2017-09-13T00:37:49.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_stream_6", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-13T00:37:49.999Z' and time <= '2017-09-13T01:37:49.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_stream_7", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-13T01:37:49.999Z' and time <= '2017-09-13T02:37:49.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_stream_8", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-13T02:37:49.999Z' and time <= '2017-09-13T03:37:49.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_stream_9", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-13T03:37:49.999Z' and time <= '2017-09-13T04:37:49.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_stream_10", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-13T04:37:49.999Z' and time <= '2017-09-13T05:37:49.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_stream_11_noData", true),
            new KeyedQuery("select * from /.*/ where time > '2017-09-13T05:37:49.999Z' and time <= '2017-09-17T16:24:10.000Z' order by time asc limit 1", "InfluxHistory_stream_12_resume", false),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T16:00:10.000Z' and time <= '2017-09-17T16:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_stream_13", true)
    ));

    private static final List<KeyedQuery> STREAM_WITH_FILTER_QUERIES = unmodifiableList(Arrays.asList(
            new KeyedQuery("select * from /.*/ order by time desc limit 1", "InfluxHistory_getEnd", false),
            new KeyedQuery("select * from /.*/ order by time asc limit 1", "InfluxHistory_getBegin", false),
            new KeyedQuery("SELECT \"Mallocs\"::field FROM \"runtime\" WHERE time > '2017-09-12T18:37:49.999Z' and time <= '2017-09-12T19:37:49.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamWithFilter_1", true),
            new KeyedQuery("SELECT \"Mallocs\"::field FROM \"runtime\" WHERE time > '2017-09-12T19:37:49.999Z' and time <= '2017-09-12T20:37:49.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamWithFilter_2", true),
            new KeyedQuery("select * from /.*/ where time > '2017-09-12T20:37:49.999Z' and time <= '2017-09-17T16:24:10.000Z' order by time asc limit 1", "InfluxHistory_streamWithFilter_3", false)
    ));

    private static final List<KeyedQuery> STREAM_WITH_BEGIN_QUERIES = unmodifiableList(Arrays.asList(
            new KeyedQuery("select * from /.*/ order by time desc limit 1", "InfluxHistory_getEnd", false),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T15:59:59.999Z' and time <= '2017-09-17T16:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamWithBegin_1", true)
    ));

    private static final List<KeyedQuery> STREAM_WITH_BEGIN_AND_END_QUERIES = unmodifiableList(Arrays.asList(
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T09:59:59.999Z' and time <= '2017-09-17T10:59:59.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamWithBeginAndEnd_1", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T10:59:59.999Z' and time <= '2017-09-17T11:59:59.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamWithBeginAndEnd_2", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T11:59:59.999Z' and time <= '2017-09-17T12:59:59.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamWithBeginAndEnd_3", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T12:59:59.999Z' and time <= '2017-09-17T13:59:59.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamWithBeginAndEnd_4", true),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T13:59:59.999Z' and time <= '2017-09-17T14:00:00.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamWithBeginAndEnd_5", true)
    ));

    private static final List<KeyedQuery> EVAL_WITH_BEGIN_AND_END_QUERIES = unmodifiableList(Arrays.asList(
            new KeyedQuery("SELECT \"NumGC\"::field FROM \"runtime\" WHERE time > '2017-09-17T09:49:59.999Z' and time <= '2017-09-17T10:49:59.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_evalWithBeginAndEnd_1", true),
            new KeyedQuery("SELECT \"NumGC\"::field FROM \"runtime\" WHERE time > '2017-09-17T10:49:59.999Z' and time <= '2017-09-17T11:49:59.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_evalWithBeginAndEnd_2", true),
            new KeyedQuery("SELECT \"NumGC\"::field FROM \"runtime\" WHERE time > '2017-09-17T11:49:59.999Z' and time <= '2017-09-17T12:49:59.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_evalWithBeginAndEnd_3", true),
            new KeyedQuery("SELECT \"NumGC\"::field FROM \"runtime\" WHERE time > '2017-09-17T12:49:59.999Z' and time <= '2017-09-17T13:49:59.999Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_evalWithBeginAndEnd_4", true),
            new KeyedQuery("SELECT \"NumGC\"::field FROM \"runtime\" WHERE time > '2017-09-17T13:49:59.999Z' and time <= '2017-09-17T14:05:00.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_evalWithBeginAndEnd_5", true)
    ));

    private static Answer<QueryResult> keyedQueriesAnswer(Collection<KeyedQuery> queries) {
        final Map<String, String> mapping = queries.stream().collect(Collectors.toMap(KeyedQuery::getQuery, KeyedQuery::getBaseFileName));

        return new Answer<QueryResult>() {
            @Override
            public QueryResult answer(InvocationOnMock invocation) throws Throwable {
                final String q = invocation.getArgumentAt(0, Query.class).getCommand();
                final String file = mapping.get(q);
                if (file != null) {
                    LOG.log(Level.INFO, "mapping to \"{1}\" from query: {0}", new Object[]{q, file});
                    return new JsonQueryResult(file).getQueryResult();
                }
                throw new AssertionError("unexpected query: " + q);
            }
        };
    }

    private static Stream<TimeSeriesCollection> computeData(Collection<KeyedQuery> queries) {
        final SeriesHandler seriesHandler = new SeriesHandler();
        queries.stream()
                .filter(KeyedQuery::isDataFragment)
                .map(KeyedQuery::getBaseFileName)
                .map(fileName -> {
                    try {
                        return new JsonQueryResult(fileName);
                    } catch (IOException ex) {
                        throw new AssertionError("missing resource: " + fileName);
                    }
                })
                .map(JsonQueryResult::getQueryResult)
                .map(qr -> qr.getResults())
                .filter(Objects::nonNull)
                .flatMap(r -> r.stream())
                .map(r -> r.getSeries())
                .filter(Objects::nonNull)
                .flatMap(s -> s.stream())
                .forEach(seriesHandler::addSeries);

        return seriesHandler.build();
    }

    private static Matcher<Iterable<? extends TimeSeriesCollection>> computeDataMatcher(Collection<KeyedQuery> queries, boolean reverse) {
        final List<Matcher<? super TimeSeriesCollection>> result = computeData(queries)
                .map(Matchers::equalTo)
                .collect(Collectors.toList());
        if (reverse) Collections.reverse(result);
        return Matchers.contains(result);
    }

    @Value
    private static class KeyedQuery {
        private final String query;
        private final String baseFileName;
        private final boolean dataFragment;
    }

    private static CollectHistory mockHistoryFromData(Stream<TimeSeriesCollection> tsdataStream) {
        final List<TimeSeriesCollection> tsdata = tsdataStream.collect(Collectors.toList());

        return new CollectHistory() {
            @Override
            public Stream<TimeSeriesCollection> stream() {
                return tsdata.stream();
            }

            @Override
            public DateTime getEnd() {
                return tsdata.get(tsdata.size() - 1).getTimestamp();
            }

            @Override
            public long getFileSize() {
                return 0;
            }

            @Override
            public boolean add(TimeSeriesCollection c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean addAll(Collection c) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
