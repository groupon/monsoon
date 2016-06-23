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
package com.groupon.lex.metrics.collector.httpget;

import com.google.gson.Gson;
import com.groupon.lex.metrics.GroupGenerator;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Metric;
import com.groupon.lex.metrics.MetricGroup;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import static java.util.Collections.singletonMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * @author ariane
 */
@RunWith(MockitoJUnitRunner.class)
public class CollectdPushCollectorTest {
    private CollectdPushCollector collectd;
    private String collectd_path;
    private HttpServlet collectd_acceptor;
    private DateTime NOW = DateTime.now(DateTimeZone.UTC);
    private String json;
    private static final GroupName UPTIME_NAME = new GroupName(new SimpleGroupPath("foo", "uptime", "0"), singletonMap("host", MetricValue.fromStrValue("localhost")));
    private static final GroupName DOWNTIME_NAME = new GroupName(new SimpleGroupPath("foo", "downtime", "0"), singletonMap("host", MetricValue.fromStrValue("localhost")));
    private static final MetricName UPTIME_METRIC = new MetricName("uptime", "0");
    private static final MetricName DOWNTIME_METRIC = new MetricName("downtime", "0");

    @Mock
    private HttpServletRequest request;
    @Mock
    private HttpServletResponse response;

    @Before
    public void setup() throws Exception {
        collectd = new CollectdPushCollector(
                (path, handler) -> {
                    collectd_path = path;
                    collectd_acceptor = handler;
                },
                new SimpleGroupPath("foo"),
                "bar");

        Gson gson = new Gson();
        CollectdPushCollector.CollectdMessage msg1 = new CollectdPushCollector.CollectdMessage();
        msg1.dsnames = Arrays.asList("value");
        msg1.dstypes = Arrays.asList("gauge");
        msg1.values = Arrays.asList(17);
        msg1.host = "localhost";
        msg1.plugin = "uptime";
        msg1.plugin_instance = "0";
        msg1.type = "uptime";
        msg1.type_instance = "0";
        msg1.interval = 60;
        msg1.time = NOW.getMillis() / 1000d;
        CollectdPushCollector.CollectdMessage msg2 = new CollectdPushCollector.CollectdMessage();
        msg2.dsnames = Arrays.asList("value");
        msg2.dstypes = Arrays.asList("gauge");
        msg2.values = Arrays.asList(-17);
        msg2.host = "localhost";
        msg2.plugin = "downtime";
        msg2.plugin_instance = "0";
        msg2.type = "downtime";
        msg2.type_instance = "0";
        msg2.interval = 60;
        msg2.time = NOW.getMillis() / 1000d;
        json = gson.toJson(Arrays.asList(msg1, msg2));
    }

    @Test
    public void getBasePath() {
        assertEquals(new SimpleGroupPath("foo"), collectd.getBasePath());
    }

    @Test
    public void correct_path_registered() {
        assertEquals("/collectd/jsonpush/bar", collectd_path);
    }

    @Test
    public void consume_data() throws Exception {
        /* Mock out classes involved in HTTP request. */
        when(request.getMethod()).thenReturn("POST");
        when(request.getCharacterEncoding()).thenReturn("UTF-8");
        when(request.getReader()).thenReturn(new BufferedReader(new StringReader(json)));
        when(response.getWriter()).thenReturn(new PrintWriter(new StringWriter()));

        /* Before data arrives, getGroups is empty. */
        GroupGenerator.GroupCollection groups = collectd.getGroups();
        assertTrue(groups.isSuccessful());
        assertTrue(groups.getGroups().isEmpty());

        /* Perform HTTP request. */
        collectd_acceptor.service(request, response);

        /* After data arrives, getGroups returns this data. */
        groups = collectd.getGroups();
        assertTrue(groups.isSuccessful());
        // Convenience map for validation.
        Map<GroupName, Metric[]> group_map = groups.getGroups().stream().map(x -> (MetricGroup)x).collect(Collectors.toMap(MetricGroup::getName, MetricGroup::getMetrics));
        assertThat(group_map, allOf(hasKey(UPTIME_NAME), hasKey(DOWNTIME_NAME)));
        assertEquals(UPTIME_METRIC, group_map.get(UPTIME_NAME)[0].getName());
        assertEquals(MetricValue.fromIntValue(17), group_map.get(UPTIME_NAME)[0].getValue());
        assertEquals(DOWNTIME_METRIC, group_map.get(DOWNTIME_NAME)[0].getName());
        assertEquals(MetricValue.fromIntValue(-17), group_map.get(DOWNTIME_NAME)[0].getValue());

        /* After data collection, data must be removed from set. */
        groups = collectd.getGroups();
        assertTrue(groups.isSuccessful());
        assertTrue(groups.getGroups().isEmpty());

        verify(response, times(1)).setStatus(200);
    }

    @Test(expected = IllegalArgumentException.class)
    public void empty_api_name_is_disallowed() throws Exception {
        new CollectdPushCollector((path, handler) -> {}, new SimpleGroupPath("foo"), "");
    }
}
