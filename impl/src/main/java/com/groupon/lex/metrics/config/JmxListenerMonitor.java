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

import com.groupon.lex.metrics.jmx.JmxClient;
import com.groupon.lex.metrics.MetricRegistryInstance;
import static com.groupon.lex.metrics.ConfigSupport.quotedString;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.jmx.MetricListenerInstance;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import java.io.IOException;
import static java.util.Collections.unmodifiableSortedSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;
import lombok.NonNull;
import lombok.Value;
import com.groupon.lex.metrics.resolver.NameBoundResolver;

@Value
public final class JmxListenerMonitor implements MonitorStatement {
    @NonNull
    private final SortedSet<ObjectName> includes;
    @NonNull
    private final NameBoundResolver tupledElements;

    public JmxListenerMonitor(@NonNull Set<ObjectName> includes, @NonNull NameBoundResolver template_args) {
        this.includes = unmodifiableSortedSet(new TreeSet<>(includes));
        this.tupledElements = template_args;
    }

    @Override
    public void apply(MetricRegistryInstance registry) throws Exception, IOException, InstanceNotFoundException {
        final Iterator<Map<Any2<Integer, String>, Any3<Boolean, Integer, String>>> argIter = tupledElements.resolve().iterator();
        while (argIter.hasNext()) {
            final Map<Any2<Integer, String>, Any3<Boolean, Integer, String>> arg = argIter.next();

            final String host = arg.getOrDefault(Any2.<Integer, String>right("host"), Any3.create3("localhost")).mapCombine(String::valueOf, String::valueOf, String::valueOf);
            final String port = arg.getOrDefault(Any2.<Integer, String>right("port"), Any3.create2(9999)).mapCombine(String::valueOf, String::valueOf, String::valueOf);

            final List<String> sublist = NameBoundResolver.indexToStringMap(arg).entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());
            final Tags tags = Tags.valueOf(NameBoundResolver.tagMap(arg));

            MetricListenerInstance listener = new MetricListenerInstance(new JmxClient("service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi"), getIncludes(), sublist, tags);
            registry.add(listener);
            listener.enable();
        }
    }

    @Override
    public StringBuilder configString() {
        final StringBuilder buf = new StringBuilder().append("collect jmx_listener");

        boolean first = true;
        for (ObjectName obj : includes) {
            if (first) {
                buf.append(' ');
                first = false;
            } else {
                buf.append(", ");
            }
            buf.append(quotedString(obj.toString()));
        }

        if (!tupledElements.isEmpty()) {
            buf
                    .append(' ')
                    .append(tupledElements.configString());
        } else {
            buf.append(';');
        }

        buf.append('\n');
        return buf;
    }
}
