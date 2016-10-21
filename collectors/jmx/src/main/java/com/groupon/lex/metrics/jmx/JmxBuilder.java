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
import com.groupon.lex.metrics.ResolverGroupGenerator;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.builders.collector.AcceptTagSet;
import com.groupon.lex.metrics.builders.collector.CollectorBuilder;
import com.groupon.lex.metrics.builders.collector.MainStringList;
import com.groupon.lex.metrics.httpd.EndpointRegistration;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import com.groupon.lex.metrics.resolver.NameBoundResolver;
import static java.util.Collections.unmodifiableSortedSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class JmxBuilder implements CollectorBuilder, MainStringList, AcceptTagSet {
    private List<String> main;
    private NameBoundResolver tagSet;

    @Override
    public GroupGenerator build(EndpointRegistration er) throws Exception {
        return new ResolverGroupGenerator(tagSet, new Constructor(getIncludes()));
    }

    private SortedSet<ObjectName> getIncludes() throws MalformedObjectNameException {
        final SortedSet<ObjectName> includes = new TreeSet<>();
        for (String name : main)
            includes.add(new ObjectName(name));
        return unmodifiableSortedSet(includes);
    }

    @RequiredArgsConstructor
    private static class Constructor implements ResolverGroupGenerator.GroupGeneratorFactory {
        private final SortedSet<ObjectName> includes;

        @Override
        public GroupGenerator create(Map<Any2<Integer, String>, Any3<Boolean, Integer, String>> arg) throws Exception {
            final String host = arg.getOrDefault(Any2.<Integer, String>right("host"), Any3.create3("localhost")).mapCombine(String::valueOf, String::valueOf, String::valueOf);
            final String port = arg.getOrDefault(Any2.<Integer, String>right("port"), Any3.create2(9999)).mapCombine(String::valueOf, String::valueOf, String::valueOf);

            final List<String> sublist = NameBoundResolver.indexToStringMap(arg).entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());
            final Tags tags = Tags.valueOf(NameBoundResolver.tagMap(arg));

            MetricListener listener = new MetricListener(new JmxClient("service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi", true), includes, sublist, tags);
            listener.enable();
            return listener;
        }
    }
}
