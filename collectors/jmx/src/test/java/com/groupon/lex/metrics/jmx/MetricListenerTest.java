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
package com.groupon.lex.metrics.jmx;

import com.groupon.lex.metrics.GroupGenerator;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricGroup;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.resolver.NamedResolverMap;
import java.lang.management.ManagementFactory;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.management.ObjectName;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class MetricListenerTest {
    private static final AtomicInteger SEQUENCE = new AtomicInteger();  // To help us create unique names for each test.
    private int seqno;
    /**
     * Group name prefix used during test.
     */
    private String PREFIX;
    /**
     * Group name prefix that does not match this test.
     */
    private String NOT_PREFIX;
    private Function<Map<String, MetricValue>, GroupName> GROUP_PATH;
    /**
     * JMX client instance.
     */
    private JmxClient jmx;

    private MetricListener listener;

    @Before
    public void setup() throws Exception {
        seqno = SEQUENCE.getAndIncrement();
        PREFIX = "com.groupon.lex.metrics.jmx:type=MetricListenerInstanceTest,seq=" + seqno + ",";
        NOT_PREFIX = "com.groupon.lex.metrics.jmx:type=NotThisMetricListenerInstanceTest,seq=" + seqno + ",";
        GROUP_PATH = new Function<Map<String, MetricValue>, GroupName>() {
            public GroupName apply(Map<String, MetricValue> extra_tags) {
                return GroupName.valueOf(SimpleGroupPath.valueOf("com", "groupon", "lex", "metrics", "jmx", "MetricListenerInstanceTest"),
                        new HashMap<String, MetricValue>() {
                    {
                        put("seq", MetricValue.fromIntValue(seqno));
                        putAll(extra_tags);
                    }
                });
            }
        };

        jmx = new JmxClient();

        listener = new MetricListener(jmx, singleton(new ObjectName(PREFIX + "*")), NamedResolverMap.EMPTY);
    }

    @After
    public void cleanup() throws Exception {
        listener.close();
        jmx.close();
    }

    @Test
    public void after_construction() throws Exception {
        assertFalse(listener.isEnabled());
        assertThat(listener.getFilter(),
                arrayContainingInAnyOrder(new ObjectName(PREFIX + "*")));
        assertEquals(0, listener.getDetectedNames().length);
    }

    @Test
    public void run_with_nothing_found() throws Exception {
        listener.enable();

        GroupGenerator.GroupCollection groups = listener.getGroups();
        assertTrue(groups.isSuccessful());
        assertTrue(groups.getGroups().isEmpty());
    }

    @Test
    public void run_with_something_found() throws Exception {
        /**
         * Test value that is exposed on local JMX.
         */
        final TestValueImpl test_value = new TestValueImpl();

        final GroupGenerator.GroupCollection groups;
        ManagementFactory.getPlatformMBeanServer().registerMBean(test_value, new ObjectName(PREFIX + "something=found"));
        try {
            listener.enable();
            groups = listener.getGroups();

            assertTrue(groups.isSuccessful());
            assertFalse(groups.getGroups().isEmpty());

            /**
             * Convenience conversion for testing.
             */
            List<GroupName> names = groups.getGroups().stream()
                    .map(MetricGroup::getName)
                    .collect(Collectors.toList());
            System.err.println(names);
            assertThat(names, contains(GROUP_PATH.apply(singletonMap("something", MetricValue.fromStrValue("found")))));
        } finally {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(PREFIX + "something=found"));
        }
    }

    @Test
    public void run_with_something_found_after_first_collection() throws Exception {
        /**
         * Test value that is exposed on local JMX.
         */
        final TestValueImpl test_value = new TestValueImpl();
        GroupGenerator.GroupCollection groups;

        listener.enable();

        groups = listener.getGroups();
        assertTrue(groups.isSuccessful());
        assertTrue("object hasn't registered yet", groups.getGroups().isEmpty());

        ManagementFactory.getPlatformMBeanServer().registerMBean(test_value, new ObjectName(PREFIX + "something=found"));
        try {
            groups = listener.getGroups();
            assertTrue(groups.isSuccessful());
            assertFalse(groups.getGroups().isEmpty());

            /**
             * Convenience conversion for testing.
             */
            List<GroupName> names = groups.getGroups().stream()
                    .map(MetricGroup::getName)
                    .collect(Collectors.toList());
            System.err.println(names);
            assertThat(names, contains(GROUP_PATH.apply(singletonMap("something", MetricValue.fromStrValue("found")))));
        } finally {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(PREFIX + "something=found"));
        }
    }

    @Test
    public void find_nothing_when_disabled() throws Exception {
        /**
         * Test value that is exposed on local JMX.
         */
        final TestValueImpl test_value = new TestValueImpl();

        GroupGenerator.GroupCollection groups;
        ManagementFactory.getPlatformMBeanServer().registerMBean(test_value, new ObjectName(PREFIX + "something=found"));
        try {
            listener.enable();
            groups = listener.getGroups();
            assertTrue(groups.isSuccessful());
            assertFalse(groups.getGroups().isEmpty());

            listener.disable();  // Test starts here.
            groups = listener.getGroups();
            assertTrue(groups.isSuccessful());
            assertTrue(groups.getGroups().isEmpty());
        } finally {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(PREFIX + "something=found"));
        }
    }

    @Test
    public void filter_works() throws Exception {
        /**
         * Test value that is exposed on local JMX.
         */
        final TestValueImpl test_value = new TestValueImpl();

        final GroupGenerator.GroupCollection groups;
        ManagementFactory.getPlatformMBeanServer().registerMBean(test_value, new ObjectName(NOT_PREFIX + "something=found"));
        try {
            listener.enable();
            groups = listener.getGroups();

            assertTrue(groups.isSuccessful());
            assertTrue("Wrongly named object is not matched by filter", groups.getGroups().isEmpty());
        } finally {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(NOT_PREFIX + "something=found"));
        }
    }
}
