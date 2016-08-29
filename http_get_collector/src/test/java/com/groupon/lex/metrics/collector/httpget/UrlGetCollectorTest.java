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

import com.groupon.lex.metrics.GroupGenerator;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Metric;
import com.groupon.lex.metrics.MetricGroup;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.lib.StringTemplate;
import com.groupon.lex.metrics.resolver.NameResolver;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.model.ConnectionOptions;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

/**
 *
 * @author ariane
 */
public class UrlGetCollectorTest {
    @Rule
    public MockServerRule mockServerRule = new MockServerRule(this);

    private MockServerClient mockServerClient;

    private UrlGetCollector endpoint(String path) {
        return new UrlGetCollector(SimpleGroupPath.valueOf("test"), new UrlPattern(StringTemplate.fromString("http://localhost:" + mockServerRule.getPort() + path), NameResolver.EMPTY));
    }

    @Test(timeout = 10000)
    public void scrape() {
        final HttpRequest REQUEST = HttpRequest.request("/scrape")
                .withMethod("GET");
        final UrlGetCollector collector = endpoint("/scrape");
        mockServerClient
                .when(REQUEST)
                .respond(HttpResponse.response("chocoladevla")
                        .withHeader("Test-Header", "Test-Response")
                        .withHeader("Double-Value", "17.1"));

        GroupGenerator.GroupCollection groups = collector.getGroups();
        mockServerClient.verify(REQUEST, VerificationTimes.once());
        assertTrue(groups.isSuccessful());

        assertThat(groups.getGroups().stream().map(MetricGroup::getName).collect(Collectors.toList()),
                Matchers.contains(GroupName.valueOf("test")));

        // Verify data in test_group.
        final Map<MetricName, MetricValue> metrics = Arrays.stream(groups.getGroups().stream()
                        .filter(mg -> mg.getName().equals(GroupName.valueOf("test")))
                        .findFirst()
                        .get()
                        .getMetrics()
                )
                .collect(Collectors.toMap(Metric::getName, Metric::getValue));
        System.err.println(metrics);
        assertThat(metrics, allOf(
                hasEntry(MetricName.valueOf("up"), MetricValue.TRUE),
                hasEntry(MetricName.valueOf("header", "Test-Header"), MetricValue.fromStrValue("Test-Response")),
                hasEntry(MetricName.valueOf("header", "Double-Value"), MetricValue.fromDblValue(17.1)),
                hasEntry(MetricName.valueOf("content", "length"), MetricValue.fromIntValue(12)),
                hasEntry(MetricName.valueOf("content", "type"), MetricValue.fromStrValue("text/plain")),
                hasEntry(MetricName.valueOf("status", "code"), MetricValue.fromIntValue(200)),
                not(hasKey(MetricName.valueOf("body")))));
    }

    @Test(timeout = 10000)
    public void scrape_without_contentlength() {
        final HttpRequest REQUEST = HttpRequest.request("/scrapeWCL")
                .withMethod("GET");
        final UrlGetCollector collector = endpoint("/scrapeWCL");
        mockServerClient
                .when(REQUEST)
                .respond(HttpResponse.response("chocoladevla")
                        .withConnectionOptions(ConnectionOptions.connectionOptions().withSuppressContentLengthHeader(true).withCloseSocket(true)));

        GroupGenerator.GroupCollection groups = collector.getGroups();
        mockServerClient.verify(REQUEST, VerificationTimes.once());
        assertTrue(groups.isSuccessful());

        assertThat(groups.getGroups().stream().map(MetricGroup::getName).collect(Collectors.toList()),
                Matchers.contains(GroupName.valueOf("test")));

        // Verify data in test_group.
        final Map<MetricName, MetricValue> metrics = Arrays.stream(groups.getGroups().stream()
                        .filter(mg -> mg.getName().equals(GroupName.valueOf("test")))
                        .findFirst()
                        .get()
                        .getMetrics()
                )
                .collect(Collectors.toMap(Metric::getName, Metric::getValue));
        System.err.println(metrics);
        assertThat(metrics, allOf(
                hasEntry(MetricName.valueOf("up"), MetricValue.TRUE),
                hasEntry(MetricName.valueOf("content", "length"), MetricValue.fromIntValue(12))));
    }
}
