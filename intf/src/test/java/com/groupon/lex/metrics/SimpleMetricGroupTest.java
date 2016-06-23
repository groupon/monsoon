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
package com.groupon.lex.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.unmodifiableList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class SimpleMetricGroupTest {
    private static final String REPLACE_NAME = "replace me";
    private static final GroupName GROUP_NAME = new GroupName(new SimpleGroupPath("com", "groupon", "lex", "jmx-monitord", "Awesomium"));

    private static final Collection<SimpleMetric> METRICES = unmodifiableList(Arrays.asList(
            new SimpleMetric(new MetricName("bool metric"), Boolean.TRUE),
            new SimpleMetric(new MetricName("int metric"), 17),
            new SimpleMetric(new MetricName("float metric"), 7F),
            new SimpleMetric(new MetricName("string metric"), "fizzbuzz"),
            new SimpleMetric(new MetricName(REPLACE_NAME), REPLACE_NAME)
        ));

    private Iterator<SimpleMetric> make_iterator() {
        return METRICES.iterator();
    }

    private Iterable<SimpleMetric> make_iterable() {
        return METRICES::iterator;
    }

    private Stream<SimpleMetric> make_stream() {
        return METRICES.stream();
    }

    private Enumeration<SimpleMetric> make_enumerable() {
        return new Enumeration<SimpleMetric>() {
            private final Iterator<SimpleMetric> iter_ = METRICES.iterator();

            @Override
            public boolean hasMoreElements() {
                return iter_.hasNext();
            }

            @Override
            public SimpleMetric nextElement() {
                return iter_.next();
            }
        };
    }

    private Set<Metric> to_set_(Metric array[]) {
        return to_set_(Arrays.asList(array));
    }

    private Set<Metric> to_set_(Collection<? extends Metric> collection) {
        Set<Metric> result = new TreeSet<>((x, y) -> x.getName().compareTo(y.getName()));  // Sorting for convenience in failure messages.
        result.addAll(collection);
        return result;
    }

    @Test
    public void constructor() {
        SimpleMetricGroup grp = new SimpleMetricGroup(GROUP_NAME);

        assertEquals(GROUP_NAME, grp.getName());
        assertEquals(to_set_(Arrays.asList()), to_set_(Arrays.asList(grp.getMetrics())));
    }

    @Test
    public void constructor_iterator() {
        SimpleMetricGroup grp = new SimpleMetricGroup(GROUP_NAME, make_iterator());

        assertEquals(GROUP_NAME, grp.getName());
        assertEquals(to_set_(METRICES), to_set_(Arrays.asList(grp.getMetrics())));
    }

    @Test
    public void constructor_iterable() {
        SimpleMetricGroup grp = new SimpleMetricGroup(GROUP_NAME, make_iterable());

        assertEquals(GROUP_NAME, grp.getName());
        assertEquals(to_set_(METRICES), to_set_(Arrays.asList(grp.getMetrics())));
    }

    @Test
    public void constructor_Stream() {
        SimpleMetricGroup grp = new SimpleMetricGroup(GROUP_NAME, make_stream());

        assertEquals(GROUP_NAME, grp.getName());
        assertEquals(to_set_(METRICES), to_set_(Arrays.asList(grp.getMetrics())));
    }

    @Test
    public void constructor_enumerable() {
        SimpleMetricGroup grp = new SimpleMetricGroup(GROUP_NAME, make_enumerable());

        assertEquals(GROUP_NAME, grp.getName());
        assertEquals(to_set_(METRICES), to_set_(Arrays.asList(grp.getMetrics())));
    }

    @Test
    public void addition() {
        final Collection<SimpleMetric> expected = new ArrayList<SimpleMetric>(METRICES) {{ add(new SimpleMetric(new MetricName("added metric"), 17)); }};
        SimpleMetricGroup grp = new SimpleMetricGroup(GROUP_NAME, METRICES);

        grp.add(new SimpleMetric(new MetricName("added metric"), 17));

        assertEquals(to_set_(expected), to_set_(grp.getMetrics()));
    }

    @Test
    public void replacing() {
        final Collection<SimpleMetric> expected = new ArrayList<>(METRICES);
        expected.removeIf((x) -> new MetricName(REPLACE_NAME).equals(x.getName()));
        expected.add(new SimpleMetric(new MetricName(REPLACE_NAME), "has been replaced"));
        SimpleMetricGroup grp = new SimpleMetricGroup(GROUP_NAME, METRICES);

        grp.add(new SimpleMetric(new MetricName(REPLACE_NAME), "has been replaced"));

        assertEquals(to_set_(expected), to_set_(grp.getMetrics()));
    }
}
