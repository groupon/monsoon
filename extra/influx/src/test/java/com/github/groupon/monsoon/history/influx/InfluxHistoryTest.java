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

import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import lombok.Value;
import org.hamcrest.Matchers;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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

        try {
            history.streamReversed().collect(Collectors.toList());
        } catch (Exception | AssertionError ex) {
            Logger.getLogger(getClass().getName()).log(Level.SEVERE, "error streaming", ex);
            throw ex;
        }

        // The end query was called once.
        verify(influxDB, times(STREAM_REVERSE_QUERIES.size())).query(
                Mockito.argThat(Matchers.hasProperty("database", Matchers.equalTo(DATABASE))),
                Mockito.eq(TimeUnit.MILLISECONDS)
        );
        verifyNoMoreInteractions(influxDB);
    }

    private static final List<KeyedQuery> STREAM_REVERSE_QUERIES = unmodifiableList(Arrays.asList(
            new KeyedQuery("select * from /.*/ order by time desc limit 1", "InfluxHistory_getEnd"),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T15:24:10.000Z' and time <= '2017-09-17T16:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_1"),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T14:24:10.000Z' and time <= '2017-09-17T15:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_2"),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T13:24:10.000Z' and time <= '2017-09-17T14:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_3"),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T12:24:10.000Z' and time <= '2017-09-17T13:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_4"),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T11:24:10.000Z' and time <= '2017-09-17T12:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_5"),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T10:24:10.000Z' and time <= '2017-09-17T11:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_6"),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-17T09:24:10.000Z' and time <= '2017-09-17T10:24:10.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_7"),
            new KeyedQuery("select * from /.*/ where time <= '2017-09-17T09:24:10.000Z' order by time desc limit 1", "InfluxHistory_streamReverse_skipQuery"),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-12T17:39:30.000Z' and time <= '2017-09-12T18:39:30.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_8_afterSkipQuery"),
            new KeyedQuery("SELECT *::field FROM /^.*$/ WHERE time > '2017-09-12T16:39:30.000Z' and time <= '2017-09-12T17:39:30.000Z' GROUP BY * ORDER BY time ASC", "InfluxHistory_streamReverse_9_afterSkipQuery"),
            new KeyedQuery("select * from /.*/ where time <= '2017-09-12T16:39:30.000Z' order by time desc limit 1", "InfluxHistory_streamReverse_TheEnd")
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

    @Value
    private static class KeyedQuery {
        private final String query;
        private final String baseFileName;
    }
}
