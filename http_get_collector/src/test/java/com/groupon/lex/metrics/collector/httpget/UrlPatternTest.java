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

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.resolver.SimpleBoundNameResolver;
import com.groupon.lex.metrics.resolver.ConstResolver;
import com.groupon.lex.metrics.resolver.NameBoundResolverSet;
import com.groupon.lex.metrics.resolver.ResolverTuple;
import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import static java.util.Collections.EMPTY_LIST;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import com.groupon.lex.metrics.resolver.NameBoundResolver;
import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;

/**
 *
 * @author ariane
 */
public class UrlPatternTest {
    private static final NameBoundResolver PARAMS =
            new NameBoundResolverSet(
                    new SimpleBoundNameResolver(
                            new SimpleBoundNameResolver.Names(Any2.left(0)),
                            new ConstResolver(
                                    new ResolverTuple(newTupleElement("foo")),
                                    new ResolverTuple(newTupleElement("bar")))),
                    new SimpleBoundNameResolver(
                            new SimpleBoundNameResolver.Names(Any2.left(1)),
                            new ConstResolver(
                                    new ResolverTuple(newTupleElement("fizz")),
                                    new ResolverTuple(newTupleElement("buzz"))))
            );
    private static final NameBoundResolver PARAMS_WITH_HOST =
            new NameBoundResolverSet(
                    new SimpleBoundNameResolver(
                            new SimpleBoundNameResolver.Names(Any2.right("host")),
                            new ConstResolver(
                                    new ResolverTuple(newTupleElement("localhost")))),
                    new SimpleBoundNameResolver(
                            new SimpleBoundNameResolver.Names(Any2.left(0)),
                            new ConstResolver(
                                    new ResolverTuple(newTupleElement("foo")),
                                    new ResolverTuple(newTupleElement("bar")))),
                    new SimpleBoundNameResolver(
                            new SimpleBoundNameResolver.Names(Any2.left(1)),
                            new ConstResolver(
                                    new ResolverTuple(newTupleElement("fizz")),
                                    new ResolverTuple(newTupleElement("buzz"))))
            );

    @Test
    public void emptyArguments() throws Exception {
        final UrlPattern pattern = new UrlPattern("foobar", NameBoundResolver.EMPTY);
        final Map<GroupName, String> result = pattern.getUrls().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertTrue(pattern.getTemplateArgs().isEmpty());
        assertThat(result, hasEntry(GroupName.valueOf(), "foobar"));
    }

    @Test
    public void emptyDiscardingArguments() throws Exception {
        final UrlPattern pattern = new UrlPattern("foobar", PARAMS);
        final Map<GroupName, String> result = pattern.getUrls().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertThat(result, hasEntry(GroupName.valueOf("foo", "fizz"), "foobar"));
        assertThat(result, hasEntry(GroupName.valueOf("foo", "buzz"), "foobar"));
        assertThat(result, hasEntry(GroupName.valueOf("bar", "fizz"), "foobar"));
        assertThat(result, hasEntry(GroupName.valueOf("bar", "buzz"), "foobar"));
    }

    @Test
    public void processOneArgument() throws Exception {
        final UrlPattern pattern = new UrlPattern("Hello $0!", PARAMS);
        final Map<GroupName, String> result = pattern.getUrls().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertThat(result, hasEntry(GroupName.valueOf("foo", "fizz"), "Hello foo!"));
        assertThat(result, hasEntry(GroupName.valueOf("foo", "buzz"), "Hello foo!"));
        assertThat(result, hasEntry(GroupName.valueOf("bar", "fizz"), "Hello bar!"));
        assertThat(result, hasEntry(GroupName.valueOf("bar", "buzz"), "Hello bar!"));
    }

    @Test
    public void processTwoArguments() throws Exception {
        final UrlPattern pattern = new UrlPattern("$0 - $1", PARAMS);
        final Map<GroupName, String> result = pattern.getUrls().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertThat(result, hasEntry(GroupName.valueOf("foo", "fizz"), "foo - fizz"));
        assertThat(result, hasEntry(GroupName.valueOf("foo", "buzz"), "foo - buzz"));
        assertThat(result, hasEntry(GroupName.valueOf("bar", "fizz"), "bar - fizz"));
        assertThat(result, hasEntry(GroupName.valueOf("bar", "buzz"), "bar - buzz"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void insufficientArguments() throws Exception {
        final UrlPattern pattern = new UrlPattern("$0 $1 $2", PARAMS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void insufficientNamedArguments() throws Exception {
        final UrlPattern pattern = new UrlPattern("$0 $1 ${host}", PARAMS);
    }

    @Test
    public void config_string() throws Exception {
        final UrlPattern pattern = new UrlPattern("$0 $1 ${host}", PARAMS_WITH_HOST);

        assertEquals("${0} ${1} ${host}", pattern.toString());
    }

    @Test
    public void get_pattern() throws Exception {
        final UrlPattern pattern = new UrlPattern("$0 $1 ${host}", PARAMS_WITH_HOST);

        assertEquals("${0} ${1} ${host}", pattern.getUrlTemplate().toString());
    }

    @Test
    public void named_argument() throws Exception {
        class Helper {
            GroupName group_(String host, String port) {
                final Map<String, MetricValue> tags = new HashMap<>();
                tags.put("host", MetricValue.fromStrValue(host));
                tags.put("port", MetricValue.fromStrValue(port));
                return GroupName.valueOf(SimpleGroupPath.valueOf(EMPTY_LIST), tags);
            }
        }
        final Helper H = new Helper();

        final NameBoundResolver params =
            new NameBoundResolverSet(
                    new SimpleBoundNameResolver(
                            new SimpleBoundNameResolver.Names(Any2.right("host")),
                            new ConstResolver(
                                    new ResolverTuple(newTupleElement("www.google.com")),
                                    new ResolverTuple(newTupleElement("www.groupon.com")))),
                    new SimpleBoundNameResolver(
                            new SimpleBoundNameResolver.Names(Any2.right("port")),
                            new ConstResolver(
                                    new ResolverTuple(newTupleElement("8080")),
                                    new ResolverTuple(newTupleElement("80"))))
            );
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
