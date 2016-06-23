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

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public class SimpleMetricGroup implements MetricGroup {
    private final GroupName name_;
    private final Map<MetricName, Metric> metrics_ = new HashMap<>();  // Actually a set, but Java's sets are pretty crummy.

    /**
     * Create a new, empty metric group.
     * @param name The name of the metric group.
     */
    public SimpleMetricGroup(GroupName name) {
        name_ = name;
    }

    /**
     * Create a metric group with the given metrics.
     * @param name The name of the metric group.
     * @param i An iterator of metrics.
     */
    public SimpleMetricGroup(GroupName name, Iterator<? extends Metric> i) {
        this(name);
        while (i.hasNext()) add(i.next());
    }

    /**
     * Create a metric group with the given metrics.
     * @param name The name of the metric group.
     * @param i A stream of metrics.
     */
    public SimpleMetricGroup(GroupName name, Stream<? extends Metric> i) {
        this(name, i.collect(Collectors.<Metric>toList()));
    }

    /**
     * Create a metric group with the given metrics.
     * @param name The name of the metric group.
     * @param m An iterable group of metrics.
     */
    public SimpleMetricGroup(GroupName name, Iterable<? extends Metric> m) {
        this(name);
        m.forEach(this::add);
    }

    /**
     * Create a metric group with the given metrics.
     * @param name The name of the metric group.
     * @param m An enumerable group of metrics.
     */
    public SimpleMetricGroup(GroupName name, Enumeration<? extends Metric> m) {
        this(name);
        while (m.hasMoreElements()) add(m.nextElement());
    }

    @Override
    public Metric[] getMetrics() {
        return metrics_.values().toArray(new Metric[0]);
    }

    /**
     * Add a new metric to the metric group.
     *
     * If the metric is already present, it will be overwritten.
     * @param m The metric to be added.
     */
    public void add(Metric m) {
        final MetricName key = m.getName();
        metrics_.put(key, m);
    }

    @Override
    public GroupName getName() { return name_; }
}
