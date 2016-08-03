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

import com.groupon.lex.metrics.TupledElements;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.lib.Any2;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.EMPTY_LIST;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class UrlPatternTest {
    private final Map<Any2<String, Integer>, Set<String>> PARAMS_MAP = new HashMap<Any2<String, Integer>, Set<String>>() {{
        put(Any2.<String, Integer>right(0), Stream.of("foo", "bar").collect(Collectors.toSet()));
        put(Any2.<String, Integer>right(1), Stream.of("fizz", "buzz").collect(Collectors.toSet()));
    }};

    private final Collection<TupledElements> PARAMS = PARAMS_MAP.entrySet().stream()
            .map(e -> {
                final TupledElements elem = new TupledElements(Collections.singletonList(e.getKey()));
                e.getValue().stream()
                        .map(Collections::singletonList)
                        .forEach(elem::addValues);
                return elem;
            })
            .collect(Collectors.toList());

    @Test
    public void emptyArguments() {
        final UrlPattern pattern = new UrlPattern("foobar", EMPTY_LIST);
        final Map<GroupName, String> result = pattern.getUrls().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertTrue(pattern.getTemplateArgs().isEmpty());
        assertThat(result, hasEntry(GroupName.valueOf(), "foobar"));
    }

    @Test
    public void emptyDiscardingArguments() {
        final UrlPattern pattern = new UrlPattern("foobar", PARAMS);
        final Map<GroupName, String> result = pattern.getUrls().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertThat(result, hasEntry(GroupName.valueOf("foo", "fizz"), "foobar"));
        assertThat(result, hasEntry(GroupName.valueOf("foo", "buzz"), "foobar"));
        assertThat(result, hasEntry(GroupName.valueOf("bar", "fizz"), "foobar"));
        assertThat(result, hasEntry(GroupName.valueOf("bar", "buzz"), "foobar"));
    }

    @Test
    public void processOneArgument() {
        final UrlPattern pattern = new UrlPattern("Hello $0!", PARAMS);
        final Map<GroupName, String> result = pattern.getUrls().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertThat(result, hasEntry(GroupName.valueOf("foo", "fizz"), "Hello foo!"));
        assertThat(result, hasEntry(GroupName.valueOf("foo", "buzz"), "Hello foo!"));
        assertThat(result, hasEntry(GroupName.valueOf("bar", "fizz"), "Hello bar!"));
        assertThat(result, hasEntry(GroupName.valueOf("bar", "buzz"), "Hello bar!"));
    }

    @Test
    public void processTwoArguments() {
        final UrlPattern pattern = new UrlPattern("$0 - $1", PARAMS);
        final Map<GroupName, String> result = pattern.getUrls().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertThat(result, hasEntry(GroupName.valueOf("foo", "fizz"), "foo - fizz"));
        assertThat(result, hasEntry(GroupName.valueOf("foo", "buzz"), "foo - buzz"));
        assertThat(result, hasEntry(GroupName.valueOf("bar", "fizz"), "bar - fizz"));
        assertThat(result, hasEntry(GroupName.valueOf("bar", "buzz"), "bar - buzz"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void insufficientArguments() {
        final UrlPattern pattern = new UrlPattern("$0 $1 $2", PARAMS);
    }

    @Test
    public void config_string() {
        final UrlPattern pattern = new UrlPattern("$0 $1 ${host}", PARAMS);

        assertEquals("${0} ${1} ${host}", pattern.toString());
    }

    @Test
    public void get_pattern() {
        final UrlPattern pattern = new UrlPattern("$0 $1 ${host}", PARAMS);

        assertEquals("${0} ${1} ${host}", pattern.getUrlTemplate().toString());
    }

    @Test
    public void named_argument() {
        class Helper {
            GroupName group_(String host, String port) {
                final Map<String, MetricValue> tags = new HashMap<>();
                tags.put("host", MetricValue.fromStrValue(host));
                tags.put("port", MetricValue.fromStrValue(port));
                return GroupName.valueOf(SimpleGroupPath.valueOf(EMPTY_LIST), tags);
            }
        }
        final Helper H = new Helper();

        final Map<Any2<String, Integer>, Set<String>> params_map = new HashMap<Any2<String, Integer>, Set<String>>() {{
                put(Any2.<String, Integer>left("host"), Stream.of("www.google.com", "www.groupon.com").collect(Collectors.toSet()));
                put(Any2.<String, Integer>left("port"), Stream.of("8080", "80").collect(Collectors.toSet()));
            }};
        final Collection<TupledElements> params = params_map.entrySet().stream()
                .map(e -> {
                    final TupledElements elem = new TupledElements(Collections.singletonList(e.getKey()));
                    e.getValue().stream()
                            .map(Collections::singletonList)
                            .forEach(elem::addValues);
                    return elem;
                })
                .collect(Collectors.toList());
        final UrlPattern pattern = new UrlPattern("http://${host}:${port}/", params);

        // This is under test.
        Map<GroupName, String> result = pattern.getUrls().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertThat(result, allOf(
                hasEntry(H.group_("www.google.com", "8080"), "http://www.google.com:8080/"),
                hasEntry(H.group_("www.google.com", "80"), "http://www.google.com:80/"),
                hasEntry(H.group_("www.groupon.com", "8080"), "http://www.groupon.com:8080/"),
                hasEntry(H.group_("www.groupon.com", "80"), "http://www.groupon.com:80/")
        ));
    }
}
