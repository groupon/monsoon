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
import java.util.Arrays;
import static java.util.Collections.EMPTY_LIST;
import java.util.Map;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

/**
 *
 * @author ariane
 */
public class UrlJsonCollectorTest {
    private static final String JSON =
            "{" +
            "  \"bool\": true," +
            "  \"int\": 7," +
            "  \"dbl\": 3.1415," +
            "  \"map\": { \"key\": \"value\" }," +
            "  \"list\": [ \"item\" ]," +
            "  \"null\": null," +
            "  \"str\": \"foobar\"" +
            "}";

    @Rule
    public MockServerRule mockServerRule = new MockServerRule(this);

    private MockServerClient mockServerClient;

    private UrlJsonCollector endpoint(String path) {
        return new UrlJsonCollector(new SimpleGroupPath("test"), new UrlPattern(StringTemplate.fromString("http://localhost:" + mockServerRule.getPort() + path), EMPTY_LIST));
    }

    @Test(timeout = 10000)
    public void scrape() {
        final HttpRequest REQUEST = HttpRequest.request("/json_scrape")
                .withMethod("GET");
        final UrlJsonCollector collector = endpoint("/json_scrape");
        mockServerClient
                .when(REQUEST)
                .respond(HttpResponse.response(JSON));

        GroupGenerator.GroupCollection groups = collector.getGroups();
        mockServerClient.verify(REQUEST, VerificationTimes.once());
        assertTrue(groups.isSuccessful());

        assertThat(groups.getGroups().stream().map(MetricGroup::getName).collect(Collectors.toList()),
                Matchers.contains(new GroupName("test")));

        // Verify data in test_group.
        final Map<MetricName, MetricValue> metrics = Arrays.stream(groups.getGroups().stream()
                        .filter(mg -> mg.getName().equals(new GroupName("test")))
                        .findFirst()
                        .get()
                        .getMetrics()
                )
                .collect(Collectors.toMap(Metric::getName, Metric::getValue));
        System.err.println(metrics);
        assertThat(metrics, allOf(
                hasEntry(new MetricName("up"), MetricValue.TRUE),
                hasEntry(new MetricName("body", "bool"), MetricValue.TRUE),
                hasEntry(new MetricName("body", "int"), MetricValue.fromIntValue(7)),
                hasEntry(new MetricName("body", "dbl"), MetricValue.fromDblValue(3.1415)),
                hasEntry(new MetricName("body", "str"), MetricValue.fromStrValue("foobar")),
                hasEntry(new MetricName("body", "map", "key"), MetricValue.fromStrValue("value")),
                hasEntry(new MetricName("body", "list", "0"), MetricValue.fromStrValue("item")),
                hasEntry(new MetricName("content", "type"), MetricValue.fromStrValue("text/plain")),
                hasEntry(new MetricName("status", "code"), MetricValue.fromIntValue(200))));
    }
}
