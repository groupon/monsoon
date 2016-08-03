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

import com.groupon.lex.metrics.JmxClient;
import com.groupon.lex.metrics.MetricRegistryInstance;
import static com.groupon.lex.metrics.ConfigSupport.quotedString;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.TupledElements;
import static com.groupon.lex.metrics.TupledElements.config_string_for_args;
import com.groupon.lex.metrics.jmx.MetricListenerInstance;
import com.groupon.lex.metrics.lib.Any2;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSortedSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;

/**
 *
 * @author ariane
 */
public final class JmxListenerMonitor implements MonitorStatement {
    private final SortedSet<ObjectName> includes_;
    private final List<TupledElements> template_args_;

    public JmxListenerMonitor(Set<ObjectName> includes, Collection<TupledElements> template_args) {
        includes_ = unmodifiableSortedSet(new TreeSet<>(includes));
        template_args_ = unmodifiableList(new ArrayList<>(template_args));
    }

    public SortedSet<ObjectName> getIncludes() { return includes_; }
    public Collection<TupledElements> getTupledElements() { return template_args_; }

    @Override
    public void apply(MetricRegistryInstance registry) throws IOException, InstanceNotFoundException {
        for (Map<Any2<String, Integer>, String> arg : TupledElements.cartesianProduct(template_args_).collect(Collectors.toList())) {
            final String host = arg.getOrDefault(Any2.<String, Integer>left("host"), "localhost");
            final String port = arg.getOrDefault(Any2.<String, Integer>left("port"), "9999");

            final Map<Integer, String> int_map = new TreeMap<>();
            final Map<String, MetricValue> string_map = new HashMap<>();
            for (Map.Entry<Any2<String, Integer>, String> arg_value : arg.entrySet()) {
                arg_value.getKey().getLeft().ifPresent(key -> {
                    string_map.put(key, MetricValue.fromStrValue(arg_value.getValue()));
                });
                arg_value.getKey().getRight().ifPresent(key -> {
                    int_map.put(key, arg_value.getValue());
                });
            }
            List<String> sublist = int_map.values().stream().collect(Collectors.toList());
            Tags tags = Tags.valueOf(string_map);

            MetricListenerInstance listener = new MetricListenerInstance(new JmxClient("service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi"), getIncludes(), sublist, tags);
            registry.add(listener);
            listener.enable();
        }
    }

    @Override
    public StringBuilder configString() {
        final StringBuilder buf = new StringBuilder().append("collect jmx_listener ");
        boolean first = true;
        for (ObjectName obj : includes_) {
            if (first)
                first = false;
            else
                buf.append(", ");
            buf.append(quotedString(obj.toString()));
        }
        if (!template_args_.isEmpty()) {
            buf.append(" {\n");
            buf.append(config_string_for_args("    ", template_args_));
            buf.append("}");
        } else {
            buf.append(';');
        }
        buf.append('\n');
        return buf;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 47 * hash + Objects.hashCode(this.includes_);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final JmxListenerMonitor other = (JmxListenerMonitor) obj;
        if (!Objects.equals(this.includes_, other.includes_)) {
            return false;
        }
        return true;
    }
}
