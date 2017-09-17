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

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.hamcrest.Matchers;
import org.influxdb.InfluxDB;
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
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InfluxHistoryTest {
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
}
