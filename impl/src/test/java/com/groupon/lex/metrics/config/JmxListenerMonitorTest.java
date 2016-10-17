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
package com.groupon.lex.metrics.config;

import com.groupon.lex.metrics.GroupGenerator;
import com.groupon.lex.metrics.MetricRegistryInstance;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.jmx.JmxBuilder;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import com.groupon.lex.metrics.resolver.NameBoundResolver;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.ObjectName;
import javax.management.remote.JMXServiceURL;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JmxListenerMonitorTest {
    private static final Logger LOG = Logger.getLogger(JmxListenerMonitorTest.class.getName());
    @Mock
    private NameBoundResolver nbr;
    @Mock
    private MetricRegistryInstance mri;

    private Set<ObjectName> one_name, two_names;
    private CollectorBuilderWrapper mon_oneName, mon_twoNames;
    /**
     * mri.add(GroupGenerator) adds to this collection.
     */
    private List<GroupGenerator> listeners;
    private ExecutorService executor;

    private static JmxBuilder newJmxBuilder(Set<ObjectName> names, NameBoundResolver resolver) {
        JmxBuilder builder = new JmxBuilder();
        builder.setMain(names.stream().map(ObjectName::toString).collect(Collectors.toList()));
        builder.setTagSet(resolver);
        return builder;
    }

    @Before
    public void setup() throws Exception {
        one_name = singleton(new ObjectName("java.lang.*:*"));
        two_names = new HashSet<ObjectName>() {
            {
                add(new ObjectName("java.lang.*:*"));
                add(new ObjectName("com.groupon.*:*"));
            }
        };

        mon_oneName = new CollectorBuilderWrapper("jmx_listener", newJmxBuilder(one_name, nbr));
        mon_twoNames = new CollectorBuilderWrapper("jmx_listener", newJmxBuilder(two_names, nbr));

        listeners = new ArrayList<>();
        when(mri.add(Mockito.isA(GroupGenerator.class))).then(invocation -> {
            final GroupGenerator g = invocation.getArgumentAt(0, GroupGenerator.class);
            listeners.add(g);
            return g;
        });

        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void cleanup() {
        for (GroupGenerator g : listeners) {
            try {
                g.close();
            } catch (Exception ex) {
                LOG.log(Level.WARNING, "unable to close listener " + g, ex);
            }
        }

        executor.shutdown();
    }

    @Test
    public void configStrings_oneName_noNBR() {
        when(nbr.isEmpty()).thenReturn(true);

        assertEquals("collect jmx_listener \"java.lang.*:*\";\n",
                mon_oneName.configString().toString());

        verify(nbr, times(1)).isEmpty();
        verifyNoMoreInteractions(nbr);
    }

    @Test
    public void configStrings_twoNames_noNBR() {
        when(nbr.isEmpty()).thenReturn(true);

        assertEquals("collect jmx_listener \"com.groupon.*:*\", \"java.lang.*:*\";\n",
                mon_twoNames.configString().toString());

        verify(nbr, times(1)).isEmpty();
        verifyNoMoreInteractions(nbr);
    }

    @Test
    public void configStrings_oneName_withNBR() {
        when(nbr.isEmpty()).thenReturn(false);
        when(nbr.configString()).thenReturn("{FOOBAR}");

        assertEquals("collect jmx_listener \"java.lang.*:*\" {FOOBAR}\n",
                mon_oneName.configString().toString());

        verify(nbr, times(1)).isEmpty();
        verify(nbr, times(1)).configString();
        verifyNoMoreInteractions(nbr);
    }

    @Test
    public void configStrings_twoNames_withNBR() {
        when(nbr.isEmpty()).thenReturn(false);
        when(nbr.configString()).thenReturn("{FOOBAR}");

        assertEquals("collect jmx_listener \"com.groupon.*:*\", \"java.lang.*:*\" {FOOBAR}\n",
                mon_twoNames.configString().toString());

        verify(nbr, times(1)).isEmpty();
        verify(nbr, times(1)).configString();
        verifyNoMoreInteractions(nbr);
    }

    @Test
    public void apply_oneName_noNBR() throws Exception {
        when(nbr.resolve()).then(invocation -> Stream.of(EMPTY_MAP));

        mon_oneName.apply(mri);
        for (GroupGenerator gg : listeners) {
            try {
                gg.getGroups(executor, new CompletableFuture<>()).get();  // Force creation of JMX collectors
            } catch (Exception ex) {
                /* skip: we just want to start them, we're not interested in the outcome. */
            }
        }

        assertThat(listeners,
                contains(
                        Matchers.hasProperty("currentGenerators", contains(
                                listener("localhost", "9999", one_name, EMPTY_LIST, Tags.EMPTY)))));

        verify(nbr, times(1)).resolve();
        verify(mri, times(1)).add(Mockito.any());
        verify(mri, atLeast(0)).getApi();
        verifyNoMoreInteractions(mri, nbr);
    }

    @Test
    public void apply_twoNames_noNBR() throws Exception {
        when(nbr.resolve()).then(invocation -> Stream.of(EMPTY_MAP));

        mon_twoNames.apply(mri);
        for (GroupGenerator gg : listeners) {
            try {
                gg.getGroups(executor, new CompletableFuture<>()).get();  // Force creation of JMX collectors
            } catch (Exception ex) {
                /* skip: we just want to start them, we're not interested in the outcome. */
            }
        }

        assertThat(listeners,
                contains(
                        Matchers.hasProperty("currentGenerators", contains(
                                listener("localhost", "9999", two_names, EMPTY_LIST, Tags.EMPTY)))));

        verify(nbr, times(1)).resolve();
        verify(mri, times(1)).add(Mockito.any());
        verify(mri, atLeast(0)).getApi();
        verifyNoMoreInteractions(mri, nbr);
    }

    @Test
    public void apply_oneName_withNBR() throws Exception {
        when(nbr.resolve()).then(invocation -> Stream.of(
                new HashMap<Any2<Integer, String>, Any3<Boolean, Integer, String>>() {
            {
                put(Any2.right("host"), Any3.create3("other.host"));
                put(Any2.right("port"), Any3.create2(99));
                put(Any2.right("extra_tag"), Any3.create3("foobar"));
            }
        },
                new HashMap<Any2<Integer, String>, Any3<Boolean, Integer, String>>() {
            {
                put(Any2.right("host"), Any3.create3("localhost"));
                put(Any2.right("port"), Any3.create3("90"));
                put(Any2.right("extra_tag"), Any3.create1(true));
            }
        }));

        mon_oneName.apply(mri);
        for (GroupGenerator gg : listeners) {
            try {
                gg.getGroups(executor, new CompletableFuture<>()).get();  // Force creation of JMX collectors
            } catch (Exception ex) {
                /* skip: we just want to start them, we're not interested in the outcome. */
            }
        }

        assertThat(listeners,
                contains(
                        Matchers.hasProperty("currentGenerators", containsInAnyOrder(
                                listener("other.host", "99", one_name, EMPTY_LIST, Tags.valueOf(new HashMap<String, MetricValue>() {
                                    {
                                        put("host", MetricValue.fromStrValue("other.host"));
                                        put("port", MetricValue.fromIntValue(99));
                                        put("extra_tag", MetricValue.fromStrValue("foobar"));
                                    }
                                })),
                                listener("localhost", "90", one_name, EMPTY_LIST, Tags.valueOf(new HashMap<String, MetricValue>() {
                                    {
                                        put("host", MetricValue.fromStrValue("localhost"));
                                        put("port", MetricValue.fromStrValue("90"));
                                        put("extra_tag", MetricValue.TRUE);
                                    }
                                }))
                        ))
                ));

        verify(nbr, times(1)).resolve();
        verify(mri, times(1)).add(Mockito.any());
        verify(mri, atLeast(0)).getApi();
        verifyNoMoreInteractions(mri, nbr);
    }

    @Test
    public void apply_twoNames_withNBR() throws Exception {
        when(nbr.resolve()).then(invocation -> Stream.of(
                new HashMap<Any2<Integer, String>, Any3<Boolean, Integer, String>>() {
            {
                put(Any2.right("host"), Any3.create3("other.host"));
                put(Any2.right("port"), Any3.create2(99));
                put(Any2.left(0), Any3.create3("foobar"));
            }
        },
                new HashMap<Any2<Integer, String>, Any3<Boolean, Integer, String>>() {
            {
                put(Any2.right("host"), Any3.create3("localhost"));
                put(Any2.right("port"), Any3.create3("90"));
                put(Any2.left(0), Any3.create3("foobar"));
            }
        }));

        mon_twoNames.apply(mri);
        for (GroupGenerator gg : listeners) {
            try {
                gg.getGroups(executor, new CompletableFuture<>()).get();  // Force creation of JMX collectors
            } catch (Exception ex) {
                /* skip: we just want to start them, we're not interested in the outcome. */
            }
        }

        assertThat(listeners,
                contains(
                        Matchers.hasProperty("currentGenerators", containsInAnyOrder(
                                listener("other.host", "99", two_names, singletonList("foobar"), Tags.valueOf(new HashMap<String, MetricValue>() {
                                    {
                                        put("host", MetricValue.fromStrValue("other.host"));
                                        put("port", MetricValue.fromIntValue(99));
                                    }
                                })),
                                listener("localhost", "90", two_names, singletonList("foobar"), Tags.valueOf(new HashMap<String, MetricValue>() {
                                    {
                                        put("host", MetricValue.fromStrValue("localhost"));
                                        put("port", MetricValue.fromStrValue("90"));
                                    }
                                }))
                        ))
                ));

        verify(nbr, times(1)).resolve();
        verify(mri, times(1)).add(Mockito.any());
        verify(mri, atLeast(0)).getApi();
        verifyNoMoreInteractions(mri, nbr);
    }

    /**
     * Create a Matcher for a listener, checking several properties.
     */
    private static Matcher<GroupGenerator> listener(String host, String port, Collection<ObjectName> filter, List<String> subPath, Tags tags) throws Exception {
        final Collection<Matcher<? super ObjectName>> filterMatchers = filter.stream()
                .map(objName -> Matchers.equalTo(objName))
                .collect(Collectors.toSet());

        final Matcher<Object> jmxUrl = Matchers.hasProperty("connection",
                Matchers.hasProperty("jmxUrl", Matchers.equalTo(Optional.of(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi")))));
        final Matcher<Object> filterProperty = Matchers.hasProperty("filter", Matchers.<ObjectName>arrayContainingInAnyOrder(filterMatchers));
        final Matcher<Object> enabled = Matchers.hasProperty("enabled", Matchers.equalTo(true));
        final Matcher<Object> subPathProperty = Matchers.hasProperty("subPath", Matchers.equalTo(subPath));
        final Matcher<Object> tagProperty = Matchers.hasProperty("tags", Matchers.equalTo(tags));

        return Matchers.allOf(jmxUrl, filterProperty, enabled, subPathProperty, tagProperty);
    }
}
