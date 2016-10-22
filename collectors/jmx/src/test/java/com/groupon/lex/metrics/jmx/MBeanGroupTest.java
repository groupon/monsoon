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

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Metric;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import com.groupon.lex.metrics.resolver.NamedResolverMap;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import static java.util.Collections.singletonMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.ObjectName;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class MBeanGroupTest {
    private static final AtomicInteger sequence = new AtomicInteger();  // To help us create unique names for each test.
    private int seqno;
    /**
     * JMX name under which test value is exposed.
     */
    private ObjectName obj_name;
    /**
     * Test value that is exposed on local JMX.
     */
    private final TestValueImpl test_value = new TestValueImpl();
    /**
     * JMX client instance.
     */
    private JmxClient jmx;
    /**
     * Groupname under which test value is recorded.
     */
    private GroupName groupname;

    private ExecutorService threadpool;

    @Before
    public void setup() throws Exception {
        seqno = sequence.getAndIncrement();
        groupname = GroupName.valueOf(
                SimpleGroupPath.valueOf("com", "groupon", "lex", "metrics", "jmx", "MBeanGroupInstanceTest"),
                new HashMap<String, MetricValue>() {
            {
                put("seq", MetricValue.fromIntValue(seqno));
                put("booltag", MetricValue.FALSE);
                put("inttag", MetricValue.fromIntValue(17));
                put("dbltag", MetricValue.fromDblValue(19.0));
                put("strtag", MetricValue.fromStrValue("str"));
            }
        });

        obj_name = new ObjectName("com.groupon.lex.metrics.jmx:type=MBeanGroupInstanceTest,seq=" + seqno + ",booltag=false,inttag=17,dbltag=19.0,strtag=str");
        ManagementFactory.getPlatformMBeanServer().registerMBean(test_value, obj_name);
        jmx = new JmxClient();

        threadpool = Executors.newSingleThreadExecutor();
    }

    @After
    public void cleanup() throws Exception {
        ManagementFactory.getPlatformMBeanServer().unregisterMBean(obj_name);
        jmx.close();
        threadpool.shutdownNow();
    }

    @Test(expected = IllegalArgumentException.class)
    public void pattern_not_allowed() throws Exception {
        new MBeanGroup(new ObjectName("com.groupon.lex.metrics.jmx:type=MBeanGroupInstanceTest,*"), NamedResolverMap.EMPTY);
    }

    @Test
    public void read() throws Exception {
        final MBeanGroup mbg = new MBeanGroup(obj_name, NamedResolverMap.EMPTY);
        test_value.setBoolval(true);
        test_value.setIntval(7);
        test_value.setStringval("foobar");
        test_value.getNested().setDblval(17);

        Metric[] metrics = mbg.getMetrics(jmx.getConnection(threadpool).get().get().getMBeanServerConnection()).get().getMetrics();

        /**
         * Create metrics map for easy test asserting.
         */
        Map<MetricName, MetricValue> metrics_map = Arrays.stream(metrics).collect(Collectors.toMap(Metric::getName, Metric::getValue));
        assertThat(metrics_map, allOf(
                hasEntry(MetricName.valueOf("Boolval"), MetricValue.TRUE),
                hasEntry(MetricName.valueOf("Intval"), MetricValue.fromIntValue(7)),
                hasEntry(MetricName.valueOf("Nested", "dblval"), MetricValue.fromDblValue(17)),
                hasEntry(MetricName.valueOf("Stringval"), MetricValue.fromStrValue("foobar"))));
    }

    @Test
    public void remembers_objname() {
        final MBeanGroup mbg = new MBeanGroup(obj_name, NamedResolverMap.EMPTY);

        assertEquals(obj_name, mbg.getMonitoredMBeanName());
    }

    @Test
    public void groupname() {
        final MBeanGroup mbg = new MBeanGroup(obj_name, NamedResolverMap.EMPTY);

        assertEquals(groupname, mbg.getName());
    }

    @Test
    public void groupname_with_tags() {
        Tags extra_tags = Tags.valueOf(singletonMap("foo", MetricValue.fromStrValue("bar")));
        GroupName expected_groupname = GroupName.valueOf(groupname.getPath(), Tags.valueOf(Stream.concat(groupname.getTags().stream(), extra_tags.stream())));
        final MBeanGroup mbg = new MBeanGroup(obj_name, new NamedResolverMap(singletonMap(Any2.right("foo"), Any3.create3("bar"))));

        assertEquals(expected_groupname, mbg.getName());
    }

    @Test
    public void groupname_with_subpath() {
        SimpleGroupPath expected_path = SimpleGroupPath.valueOf(Stream.concat(groupname.getPath().getPath().stream(), Stream.of("foo")).collect(Collectors.toList()));
        GroupName expected_groupname = GroupName.valueOf(expected_path, groupname.getTags());
        final MBeanGroup mbg = new MBeanGroup(obj_name, new NamedResolverMap(singletonMap(Any2.left(0), Any3.create3("foo"))));

        assertEquals(expected_groupname, mbg.getName());
    }
}
